package org.neo4j.plugin.configuration;

import org.apache.commons.configuration2.ImmutableConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.neo4j.logging.NullLog;
import org.neo4j.plugin.configuration.listners.ConfigurationLifecycleListener;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ConfigurationLifecycleTest {

    private static File fromResource(String fileName) {
        try {
            return new File(Thread.currentThread()
                    .getContextClassLoader()
                    .getResource("test.properties")
                    .toURI());
        } catch (URISyntaxException e) {
            return null;
        }
    }
    
    private static final File FILE = fromResource("test.properties");
    private static final int TRIGGER_PERIOD_MILLIS = 100;

    private ConfigurationLifecycle configurationLifecycle;

    @Before
    public void before() {
        configurationLifecycle = new ConfigurationLifecycle(TRIGGER_PERIOD_MILLIS,
                FILE.getAbsolutePath(), true, NullLog.getInstance());
    }

    @After
    public void after() throws InterruptedException {
        if (configurationLifecycle != null) {
            configurationLifecycle.stop(true);
            Thread.sleep(1000);
        }
    }

    @Test
    public void testReloadFile() throws Exception {
        long timestamp = System.currentTimeMillis();
        String newKey = String.format("my.prop.%d", timestamp);
        String newValue = UUID.randomUUID().toString();
        CountDownLatch counterInitialized = new CountDownLatch(2);
        CountDownLatch counterChanged = new CountDownLatch(1);
        configurationLifecycle.addConfigurationLifecycleListener(EventType.CONFIGURATION_INITIALIZED, (evt, conf) -> {
            counterInitialized.countDown();
            if (evt != EventType.CONFIGURATION_INITIALIZED) return; // if not the real event we skip the assert
            Assert.assertNull("newKey", conf.getString(newKey));
        });
        configurationLifecycle.addConfigurationLifecycleListener(EventType.CONFIGURATION_CHANGED, (evt, conf) -> {
            counterChanged.countDown();
            Assert.assertEquals(newValue, conf.getString(newKey));
        });
        configurationLifecycle.start();
        Thread.sleep(2000);
        // this will trigger the following tree
        // "PROPERTY_ADDED" -> "CONFIGURATION_CHANGED" -> "CONFIGURATION_INITIALIZED"
        // this means that "CONFIGURATION_INITIALIZED" will triggered twice
        writeToFile(String.format("%s=%s", newKey, newValue));
        counterInitialized.await(30, TimeUnit.SECONDS);
        counterChanged.await(30, TimeUnit.SECONDS);
        Assert.assertEquals(0, counterInitialized.getCount());
        Assert.assertEquals(0, counterChanged.getCount());
        configurationLifecycle.stop();
        assertEnvVars();
    }

    private void assertEnvVars() throws ConfigurationException {
        final Map<String, Object> neo4jEnvVars = ConfigurationLifecycleUtils.getNeo4jEnvVars(Collections.emptyList());
        if (!neo4jEnvVars.isEmpty()) {
            System.out.println("neo4jEnvVars = " + neo4jEnvVars);
            // if `NEO4J_*` env vars are present check that are correctly imported
            final ImmutableConfiguration configuration = configurationLifecycle.getConfiguration();
            neo4jEnvVars.forEach((k, v) -> Assert.assertEquals(v, configuration.getProperty(k)));
        }
    }

    @Test
    public void testReloadFileAddBlankLine() throws Exception {
        AtomicInteger countConfigurationChanged = new AtomicInteger();
        AtomicInteger countNone = new AtomicInteger();
        configurationLifecycle.addConfigurationLifecycleListener(EventType.CONFIGURATION_CHANGED, (evt, conf) -> {
            countConfigurationChanged.incrementAndGet();
        });
        configurationLifecycle.addConfigurationLifecycleListener(EventType.NONE, (evt, conf) -> {
            countNone.incrementAndGet();
        });
        configurationLifecycle.start();
        Thread.sleep(2000);
        writeToFile(" ");
        Thread.sleep(2000);
        Assert.assertEquals(0, countConfigurationChanged.get());
        Assert.assertEquals(0, countNone.get());
        configurationLifecycle.stop();
        assertEnvVars();
    }

    @Test
    public void testSetAPIs() throws Exception {
        long timestamp = System.currentTimeMillis();
        String newKey = String.format("my.prop.%d", timestamp);
        String newValue = UUID.randomUUID().toString();
        CountDownLatch countDownLatch = new CountDownLatch(2);
        configurationLifecycle.addConfigurationLifecycleListener(EventType.CONFIGURATION_INITIALIZED, (evt, conf) -> {
            countDownLatch.countDown();
            if (evt != EventType.CONFIGURATION_INITIALIZED) return; // if not the real event we skip the assert
            Assert.assertNull("newKey", conf.getString(newKey));
        });
        configurationLifecycle.addConfigurationLifecycleListener(EventType.CONFIGURATION_CHANGED, (evt, conf) -> {
            countDownLatch.countDown();
            Assert.assertEquals(newValue, conf.getString(newKey));
        });
        configurationLifecycle.start();
        Thread.sleep(2000);
        // this will trigger the following tree
        // "PROPERTY_ADDED" -> "CONFIGURATION_CHANGED" -> "CONFIGURATION_INITIALIZED"
        // this means that "CONFIGURATION_INITIALIZED" will triggered twice
        configurationLifecycle.setProperty(newKey, newValue);
        countDownLatch.await(30, TimeUnit.SECONDS);
        Assert.assertEquals(0, countDownLatch.getCount());
        configurationLifecycle.stop();
        assertEnvVars();
    }

    @Test
    public void testSetAPIsShouldNotBeInvoked() throws Exception {
        AtomicInteger countConfigurationChanged = new AtomicInteger();
        CountDownLatch countdownNone = new CountDownLatch(1);
        configurationLifecycle.addConfigurationLifecycleListener(EventType.CONFIGURATION_CHANGED, (evt, conf) -> {
            countConfigurationChanged.incrementAndGet();
        });
        configurationLifecycle.addConfigurationLifecycleListener(EventType.NONE, (evt, conf) -> {
            countdownNone.countDown();
        });
        configurationLifecycle.start();
        // `foo=bar` already exists into the property
        // so a writing the same value fo the key
        // should not affect the `CONFIGURATION_CHANGED`
        // but invoking instead `NONE`
        configurationLifecycle.setProperty("foo", "bar");
        countdownNone.await(30, TimeUnit.SECONDS);
        Assert.assertEquals(0, countConfigurationChanged.get());
        Assert.assertEquals(0, countdownNone.getCount());
        configurationLifecycle.stop();
        assertEnvVars();
    }

    @Test
    public void testReloadFileWithRestart() throws Exception {
        long timestamp = System.currentTimeMillis();
        String newKey = String.format("my.prop.%d", timestamp);
        String newValue = UUID.randomUUID().toString();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        configurationLifecycle.addConfigurationLifecycleListener(EventType.CONFIGURATION_CHANGED, (evt, conf) -> {
            countDownLatch.countDown();
            Assert.assertEquals(newValue, conf.getString(newKey));
        });
        configurationLifecycle.start();
        Thread.sleep(2000);
        configurationLifecycle.stop();
        Thread.sleep(2000);
        configurationLifecycle.start();
        Thread.sleep(2000);
        writeToFile(String.format("%s=%s", newKey, newValue));
        countDownLatch.await(60, TimeUnit.SECONDS);
        Assert.assertEquals(0, countDownLatch.getCount());
        configurationLifecycle.stop();
        assertEnvVars();
    }

    @Test
    public void testSetPropertyReloadFile() throws Exception {
        String newKey = String.format("my.prop.%d", System.currentTimeMillis());
        String newValue = UUID.randomUUID().toString();
        Thread.sleep(2000);
        String otherNewKey = String.format("my.prop.%d", System.currentTimeMillis());
        String otherNewValue = UUID.randomUUID().toString();
        CountDownLatch countDownLatch = new CountDownLatch(2);
        configurationLifecycle.addConfigurationLifecycleListener(EventType.CONFIGURATION_CHANGED, (evt, conf) -> {
            countDownLatch.countDown();
            int count = (int) countDownLatch.getCount();
            switch (count) {
                case 1:
                    // the configuration should include both two properties
                    Assert.assertNull("newValue", conf.getString(newKey));
                    Assert.assertEquals(otherNewValue, conf.getString(otherNewKey));
                    break;
                case 0:
                    // the configuration should include both two properties
                    Assert.assertEquals(newValue, conf.getString(newKey));
                    Assert.assertEquals(otherNewValue, conf.getString(otherNewKey));
                    break;
            }
        });
        configurationLifecycle.start();
        Thread.sleep(2000);
        configurationLifecycle.setProperty(otherNewKey, otherNewValue);
        Thread.sleep(2000);
        configurationLifecycle.stop();
        Thread.sleep(2000);
        configurationLifecycle.start();
        Thread.sleep(2000);
        writeToFile(String.format("%s=%s", newKey, newValue));
        countDownLatch.await(30, TimeUnit.SECONDS);
        Assert.assertEquals(0, countDownLatch.getCount());
        configurationLifecycle.stop();
        assertEnvVars();
    }

    private void writeToFile(String format) throws IOException {
        try (FileWriter fw = new FileWriter(FILE, true);
             BufferedWriter bw = new BufferedWriter(fw)) {
            bw.write(format);
            bw.newLine();
        }
    }

    @Test
    public void testOnShutdownInvocation() throws Exception {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        configurationLifecycle.addConfigurationLifecycleListener(EventType.CONFIGURATION_CHANGED, new ConfigurationLifecycleListener() {
            @Override
            public void onConfigurationChange(EventType event, ImmutableConfiguration config) {

            }

            @Override
            public void onShutdown() {
                countDownLatch.countDown();
            }
        });
        configurationLifecycle.start();
        configurationLifecycle.stop(true);
        countDownLatch.await(30, TimeUnit.SECONDS);
        Assert.assertEquals(0, countDownLatch.getCount());
        assertEnvVars();
    }
}
