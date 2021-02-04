package org.neo4j.plugin.configuration;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.ImmutableConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.ConfigurationBuilderEvent;
import org.apache.commons.configuration2.builder.ReloadingFileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.FileBasedBuilderParameters;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.reloading.PeriodicReloadingTrigger;
import org.apache.commons.configuration2.reloading.ReloadingController;
import org.apache.commons.configuration2.sync.ReadWriteSynchronizer;
import org.neo4j.logging.Log;
import org.neo4j.plugin.configuration.listners.ConfigurationLifecycleListener;
import org.neo4j.plugin.configuration.listners.ShutdownListener;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class ConfigurationLifecycle implements AutoCloseable {

    private final int triggerPeriodMillis;
    private final Map<String, List<ConfigurationLifecycleListener>> listenerMap;
    private final PeriodicReloadingTrigger trigger;
    private final ReloadingFileBasedConfigurationBuilder<FileBasedConfiguration> builder;
    private final ScheduledExecutorService scheduledExecutorService;
    private final ExecutorService listenerExecutorService;

    private final Object lifecycleMonitor = new Object();
    private final Object listenerMonitor = new Object();
    private final AtomicReference<ImmutableConfiguration> configurationReference;
    private ScheduledFuture<?> scheduledFuture;
    private final List<String> supportedEnvVarPrefixes;
    private final Log log;
    private final AtomicBoolean firstStart;

    public ConfigurationLifecycle(int triggerPeriodMillis,
                                  String configFileName,
                                  boolean allowFailOnInit,
                                  Log log) {
        this(triggerPeriodMillis, configFileName, allowFailOnInit, log, false, null);
    }

    public ConfigurationLifecycle(int triggerPeriodMillis,
                                  String configFileName,
                                  boolean allowFailOnInit,
                                  Log log,
                                  boolean loadEnvVars,
                                  String... supportedEnvVarPrefixes) {
        listenerMap = new ConcurrentHashMap<>();
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        listenerExecutorService = Executors.newFixedThreadPool(10);
        configurationReference = new AtomicReference<>();
        this.firstStart = new AtomicBoolean(true);
        this.triggerPeriodMillis = triggerPeriodMillis;
        this.supportedEnvVarPrefixes = supportedEnvVarPrefixes == null || supportedEnvVarPrefixes.length == 0 ?
                Collections.emptyList() : Collections.unmodifiableList(Arrays.asList(supportedEnvVarPrefixes));
        this.log = log;
        File propertiesFile = new File(configFileName);
        final FileBasedBuilderParameters params = new Parameters()
                .fileBased()
                .setFile(propertiesFile)
                .setSynchronizer(new ReadWriteSynchronizer());
        builder = new ReloadingFileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class, null, allowFailOnInit)
                        .configure(params);
        if (loadEnvVars) {
            initEnvVars();
        }
        addBuilderListener();
        final ReloadingController reloadingController = builder.getReloadingController();
        trigger = new PeriodicReloadingTrigger(reloadingController,
                null, triggerPeriodMillis, TimeUnit.MILLISECONDS);
    }

    private void addBuilderListener() {
        builder.addEventListener(ConfigurationBuilderEvent.ANY,
                event -> {
                    try {
                        final boolean isResultCreated = event.getEventType().getName().equals("RESULT_CREATED");
                        if (trigger.isRunning() && (isResultCreated || this.firstStart.get())) {
                            firstStart.set(false);
                            final ImmutableConfiguration newConfiguration = event.getSource().getConfiguration();
                            ImmutableConfiguration oldConfiguration = configurationReference.get();
                            invokeListeners(oldConfiguration, newConfiguration);
                        }
                    } catch (Exception ignored) {
                        log.warn("Cannot invoke listeners because the following exception:", ignored);
                    }
                });
    }

    private void initEnvVars() {
        try {
            final Map<String, Object> neo4jEnvVars = ConfigurationLifecycleUtils.getNeo4jEnvVars(supportedEnvVarPrefixes);
            if (!neo4jEnvVars.isEmpty()) {
                log.info("Configuration has found the following environment variables: %s", neo4jEnvVars);
                final FileBasedConfiguration configuration = builder.getConfiguration();
                neo4jEnvVars.forEach(configuration::setProperty);
                checkAndSave(true);
            }
        } catch (Exception ignored) {
            log.warn("Error while we're persisting the environment variables because the following exception:", ignored);
        }
    }

    private void invokeListeners(ImmutableConfiguration oldConfig, ImmutableConfiguration newConfig) {
        synchronized (listenerMonitor) {
            final EventType eventType = ConfigurationLifecycleUtils.getEventType(newConfig, oldConfig);
            if (log.isDebugEnabled()) {
                log.debug("Detected new event change in configuration %s", eventType.name());
            }
            // we notify all the super types, so we retrieve the full dependency tree
            final List<EventType> evtTree = eventType.getTree();
            if (log.isDebugEnabled()) {
                final Collector<CharSequence, ?, String> joining = Collectors.joining(",");
                log.debug("The event tree is [%s]", evtTree.stream()
                        .map(Enum::name)
                        .collect(joining));
                log.debug("The listenerMap contains the following listeners: [%s]", listenerMap.keySet().stream()
                        .collect(joining));
            }
            evtTree.forEach(evt -> listenerMap.getOrDefault(evt.toString(), Collections.emptyList())
                    // for each event of the tree we sent the actual type, not the super type
                    .forEach(listener -> listener.onConfigurationChange(eventType, newConfig)));
            configurationReference.set(newConfig);
        }
    }

    public void setProperty(String key, Object value) throws ConfigurationException {
        setProperty(key, value, true);
    }

    public void setProperty(String key, Object value, boolean save) throws ConfigurationException {
        final ImmutableConfiguration oldConfig = ConfigurationUtils.cloneConfiguration(builder.getConfiguration());
        final FileBasedConfiguration configuration = builder.getConfiguration();
        configuration.setProperty(key, value);
        listenerExecutorService.submit(() -> invokeListeners(oldConfig, ConfigurationUtils.unmodifiableConfiguration(configuration)));
        checkAndSave(save);
    }

    public void setProperties(Map<String, Object> properties) throws ConfigurationException {
        setProperties(properties, true);
    }

    public void setProperties(Map<String, Object> properties, boolean save) throws ConfigurationException {
        final ImmutableConfiguration oldConfig = ConfigurationUtils.cloneConfiguration(builder.getConfiguration());
        final FileBasedConfiguration configuration = builder.getConfiguration();
        properties.forEach(configuration::setProperty);
        listenerExecutorService.submit(() -> invokeListeners(oldConfig, ConfigurationUtils.unmodifiableConfiguration(configuration)));
        checkAndSave(save);
    }

    private void checkAndSave(boolean save) throws ConfigurationException {
        if (save) {
            log.info("Saving the configuration back to the file");
            builder.save();
        }
    }

    public void removeProperty(String key) throws ConfigurationException {
        removeProperty(key, true);
    }

    public void removeProperty(String key, boolean save) throws ConfigurationException {
        final ImmutableConfiguration oldConfig = ConfigurationUtils.cloneConfiguration(builder.getConfiguration());
        final FileBasedConfiguration configuration = builder.getConfiguration();
        configuration.clearProperty(key);
        listenerExecutorService.submit(() -> invokeListeners(oldConfig, ConfigurationUtils.unmodifiableConfiguration(configuration)));
        checkAndSave(save);
    }

    public void removeProperties(Collection<String> properties) throws ConfigurationException {
        removeProperties(properties, true);
    }

    public void removeProperties(Collection<String> properties, boolean save) throws ConfigurationException {
        final ImmutableConfiguration oldConfig = ConfigurationUtils.cloneConfiguration(builder.getConfiguration());
        final FileBasedConfiguration configuration = builder.getConfiguration();
        properties.forEach(configuration::clearProperty);
        listenerExecutorService.submit(() -> invokeListeners(oldConfig, ConfigurationUtils.unmodifiableConfiguration(configuration)));
        checkAndSave(save);
    }

    public ImmutableConfiguration getConfiguration() throws ConfigurationException {
        final Configuration configuration = ConfigurationUtils.cloneConfiguration(builder.getConfiguration());
        return ConfigurationUtils.unmodifiableConfiguration(configuration);
    }

    public void addConfigurationLifecycleListener(EventType eventType, ConfigurationLifecycleListener listener) {
        log.info("Adding listener for event type %s", eventType.name());
        listenerMap.compute(eventType.toString(), (id, listeners) -> {
            if (listeners == null) {
                listeners = new ArrayList<>();
            }
            listeners.add(listener);
            return listeners;
        });
    }

    public void removeConfigurationLifecycleListener(EventType eventType, ConfigurationLifecycleListener listener) {
        log.info("Removing listener for event type %s", eventType.name());
        listenerMap.computeIfPresent(eventType.toString(), (id, listeners) -> {
            listeners.remove(listener);
            return listeners;
        });
    }

    public boolean isRunning() {
        return trigger.isRunning();
    }

    public void start() {
        synchronized (lifecycleMonitor) {
            if (isRunning()) {
                return;
            }
            log.info("Starting the connector lifecycle listener");
            trigger.start();
            // this is a workaround because the configuration2
            // framework reloads the configuration on demand
            // when you ask for a configuration value
            scheduledFuture = scheduledExecutorService.scheduleAtFixedRate(() -> reload(),
                    triggerPeriodMillis, triggerPeriodMillis,
                    TimeUnit.MILLISECONDS);
        }
    }

    public void reload() {
        try {
            builder.getConfiguration().getString("");
        } catch (Exception e) {
            log.warn("Cannot reload the configuration because of the following exception:", e);
        }
    }

    public void stop() {
        stop(false);
    }

    public void stop(boolean shutdown) {
        synchronized (lifecycleMonitor) {
            if (isRunning()) {
                log.info("Stopping the connector lifecycle listener with shutdown: %s", shutdown);
            }
            trigger.shutdown(shutdown);
            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
                scheduledFuture = null;
                if (shutdown) {
                    scheduledExecutorService.shutdown();
                }
            }
            if (shutdown) {
                listenerExecutorService.shutdown();
                this.listenerMap.values()
                        .stream()
                        .flatMap(Collection::stream)
                        .forEach(ShutdownListener::onShutdown);
            }
        }
    }

    @Override
    public void close() throws Exception {
        stop(true);
    }
}
