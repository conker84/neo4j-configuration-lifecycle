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
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Hello world!
 */
public class ConfigurationLifecycle implements AutoCloseable {

    private final int triggerPeriodMillis;
    private final String configFileName;
    private final List<ConfigurationLifecycleListener> listeners = new ArrayList<>();
    private final PeriodicReloadingTrigger trigger;
    private final ReloadingFileBasedConfigurationBuilder<FileBasedConfiguration> builder;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    private final Object monitor = new Object();

    public ConfigurationLifecycle(int triggerPeriodMillis, String configFileName) {
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
                            final ImmutableConfiguration configuration = event.getSource().getConfiguration();
                            invokeListeners(event.getEventType().getName(), configuration);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        final ReloadingController reloadingController = builder.getReloadingController();
        trigger = new PeriodicReloadingTrigger(reloadingController,
                null, triggerPeriodMillis, TimeUnit.MILLISECONDS);
    }

    private void invokeListeners(String event, ImmutableConfiguration configuration) {
        listeners.forEach(listener -> listener.onConfigurationChange(event, configuration));
    }

    public void setProperty(String key, Object value) throws ConfigurationException {
        final FileBasedConfiguration configuration = builder.getConfiguration();
        configuration.setProperty(key, value);
        invokeListeners("RESULT_CREATED", ConfigurationUtils.unmodifiableConfiguration(configuration));
    }

    public void addConfigurationLifecycleListener(ConfigurationLifecycleListener listener) {
        this.listeners.add(listener);
    }

    public boolean removeConfigurationLifecycleListener(ConfigurationLifecycleListener listener) {
        return this.listeners.remove(listener);
    }

    public boolean isRunning() {
        return trigger.isRunning() && !executorService.isShutdown();
    }

    public void start() {
        synchronized (monitor) {
            trigger.start();
            // this is a workaround because the configuration2
            // framework reloads the configuration on demand
            // when you ask for a configuration value
            executorService.submit(() -> {
                while (true) {
                    try {
                        builder.getConfiguration().getString("");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    Thread.sleep(triggerPeriodMillis);
                }
            });
        }
    }

    public void stop() {
        synchronized (monitor) {
            trigger.shutdown(true);
            executorService.shutdown();
        }
    }

    @Override
    public void close() throws Exception {
        stop();
    }
}
