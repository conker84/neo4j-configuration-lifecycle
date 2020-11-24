package org.neo4j.plugin.configuration;

import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.ImmutableConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.ConfigurationBuilderEvent;
import org.apache.commons.configuration2.builder.ReloadingFileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.reloading.PeriodicReloadingTrigger;
import org.apache.commons.configuration2.reloading.ReloadingController;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Hello world!
 */
public class ConfigurationLifecycle implements AutoCloseable {

    private final int triggerPeriodMillis;
    private final String configFileName;
    private final Map<String, List<ConfigurationLifecycleListener>> listenerMap;
    private final PeriodicReloadingTrigger trigger;
    private final ReloadingFileBasedConfigurationBuilder<FileBasedConfiguration> builder;
    private final ExecutorService executorService;

    private final Object lifecycleMonitor = new Object();
    private final Object configurationMonitor = new Object();
    private final AtomicReference<ImmutableConfiguration> configurationReference;

    public ConfigurationLifecycle(int triggerPeriodMillis, String configFileName) {
        listenerMap = new ConcurrentHashMap<>();
        executorService = Executors.newSingleThreadExecutor();
        configurationReference = new AtomicReference<>();
        this.triggerPeriodMillis = triggerPeriodMillis;
        this.configFileName = configFileName;

        Parameters params = new Parameters();
        File propertiesFile = new File(configFileName);

        builder = new ReloadingFileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class)
                        .configure(params.fileBased().setFile(propertiesFile));
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
        List<EventType> evtTree = ConfigurationLifecycleUtils.getEventType(newConfig, oldConfig).getTree();
        evtTree.forEach(evt -> listenerMap.getOrDefault(evt.toString(), Collections.emptyList())
                    .forEach(listener -> listener.onConfigurationChange(evt, newConfig)));
        configurationReference.set(newConfig);
    }

    public void setProperty(String key, Object value) throws ConfigurationException {
        final ImmutableConfiguration oldConfig = ConfigurationUtils.cloneConfiguration(builder.getConfiguration());
        final FileBasedConfiguration configuration = builder.getConfiguration();
        configuration.setProperty(key, value);
        invokeListeners(oldConfig, ConfigurationUtils.unmodifiableConfiguration(configuration));
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
        return trigger.isRunning() && !executorService.isShutdown();
    }

    public void start() {
        synchronized (lifecycleMonitor) {
            trigger.start();
            // this is a workaround because the configuration2
            // framework reloads the configuration on demand
            // when you ask for a configuration value
            executorService.submit(() -> {
                while (true) {
                    reload();
                    Thread.sleep(triggerPeriodMillis);
                }
            });
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
        synchronized (lifecycleMonitor) {
            trigger.shutdown(true);
            executorService.shutdown();
        }
    }

    @Override
    public void close() throws Exception {
        stop();
    }
}