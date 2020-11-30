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
import org.neo4j.plugin.configuration.listners.ConfigurationLifecycleListener;
import org.neo4j.plugin.configuration.listners.ShutdownListener;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class ConfigurationLifecycle implements AutoCloseable {

    private final int triggerPeriodMillis;
    private final Map<String, List<ConfigurationLifecycleListener>> listenerMap;
    private final PeriodicReloadingTrigger trigger;
    private final ReloadingFileBasedConfigurationBuilder<FileBasedConfiguration> builder;
    private final ScheduledExecutorService executorService;

    private final Object lifecycleMonitor = new Object();
    private final Object configurationMonitor = new Object();
    private final AtomicReference<ImmutableConfiguration> configurationReference;
    private ScheduledFuture<?> scheduledFuture;

    public ConfigurationLifecycle(int triggerPeriodMillis, String configFileName, boolean allowFailOnInit) {
        listenerMap = new ConcurrentHashMap<>();
        executorService = Executors.newSingleThreadScheduledExecutor();
        configurationReference = new AtomicReference<>();
        this.triggerPeriodMillis = triggerPeriodMillis;

        File propertiesFile = new File(configFileName);
        final FileBasedBuilderParameters params = new Parameters()
                .fileBased()
                .setFile(propertiesFile)
                .setSynchronizer(new ReadWriteSynchronizer());
        builder = new ReloadingFileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class, null, allowFailOnInit)
                        .configure(params);
        builder.addEventListener(ConfigurationBuilderEvent.ANY,
                event -> {
                    try {
                        if (event.getEventType().getName().equals("RESULT_CREATED")) {
                            synchronized (configurationMonitor) {
                                final ImmutableConfiguration configuration = event.getSource().getConfiguration();
                                invokeListeners(configurationReference.get(), configuration);
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        final ReloadingController reloadingController = builder.getReloadingController();
        trigger = new PeriodicReloadingTrigger(reloadingController,
                null, triggerPeriodMillis, TimeUnit.MILLISECONDS);
    }

    private void invokeListeners(ImmutableConfiguration oldConfig, ImmutableConfiguration newConfig) {
        final EventType eventType = ConfigurationLifecycleUtils.getEventType(newConfig, oldConfig);
        // we notify all the super types, so we retrieve the full dependency tree
        final List<EventType> evtTree = eventType.getTree();
        evtTree.forEach(evt -> listenerMap.getOrDefault(evt.toString(), Collections.emptyList())
                    // for each event of the tree we sent the actual type, not the super type
                    .forEach(listener -> listener.onConfigurationChange(eventType, newConfig)));
        configurationReference.set(newConfig);
    }

    public void setProperty(String key, Object value) throws ConfigurationException {
        setProperty(key, value, true);
    }

    public void setProperty(String key, Object value, boolean save) throws ConfigurationException {
        final ImmutableConfiguration oldConfig = ConfigurationUtils.cloneConfiguration(builder.getConfiguration());
        final FileBasedConfiguration configuration = builder.getConfiguration();
        configuration.setProperty(key, value);
        invokeListeners(oldConfig, ConfigurationUtils.unmodifiableConfiguration(configuration));
        checkAndSave(save);
    }

    public void checkAndSave(boolean save) throws ConfigurationException {
        if (save) {
            builder.save();
        }
    }

    public void setProperties(Map<String, Object> properties) throws ConfigurationException {
        setProperties(properties, true);
    }

    public void setProperties(Map<String, Object> properties, boolean save) throws ConfigurationException {
        final ImmutableConfiguration oldConfig = ConfigurationUtils.cloneConfiguration(builder.getConfiguration());
        final FileBasedConfiguration configuration = builder.getConfiguration();
        properties.forEach(configuration::setProperty);
        invokeListeners(oldConfig, ConfigurationUtils.unmodifiableConfiguration(configuration));
        checkAndSave(save);
    }

    public ImmutableConfiguration getConfiguration() throws ConfigurationException {
        final Configuration configuration = ConfigurationUtils.cloneConfiguration(builder.getConfiguration());
        return ConfigurationUtils.unmodifiableConfiguration(configuration);
    }

    public void addConfigurationLifecycleListener(EventType eventType, ConfigurationLifecycleListener listener) {
        listenerMap.compute(eventType.toString(), (id, listeners) -> {
            if (listeners == null) {
                listeners = new ArrayList<>();
            }
            listeners.add(listener);
            return listeners;
        });
    }

    public void removeConfigurationLifecycleListener(EventType eventType, ConfigurationLifecycleListener listener) {
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
            trigger.start();
            // this is a workaround because the configuration2
            // framework reloads the configuration on demand
            // when you ask for a configuration value
            scheduledFuture = executorService.scheduleAtFixedRate(() -> reload(),
                    triggerPeriodMillis, triggerPeriodMillis,
                    TimeUnit.MILLISECONDS);
        }
    }

    public void reload() {
        try {
            builder.getConfiguration().getString("");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void stop() {
        stop(false);
    }

    public void stop(boolean shutdown) {
        synchronized (lifecycleMonitor) {
            if (trigger != null) {
                trigger.shutdown(shutdown);
            }
            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
                scheduledFuture = null;
                if (shutdown) {
                    executorService.shutdown();
                }
            }
            if (shutdown) {
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
