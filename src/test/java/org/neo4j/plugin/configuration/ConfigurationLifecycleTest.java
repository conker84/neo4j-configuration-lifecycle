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

import java.io.*;
import java.net.URISyntaxException;
import java.util.*;
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
        CountDownLatch countDownNone = new CountDownLatch(1);
        configurationLifecycle.addConfigurationLifecycleListener(EventType.CONFIGURATION_CHANGED, (evt, conf) -> {
            countConfigurationChanged.incrementAndGet();
        });
        configurationLifecycle.addConfigurationLifecycleListener(EventType.NONE, (evt, conf) -> {
            countDownNone.countDown();
        });
        configurationLifecycle.start();
        Thread.sleep(2000);
        writeToFile(" ");
        countDownNone.await(10, TimeUnit.SECONDS);
        Assert.assertEquals(0, countConfigurationChanged.get());
        Assert.assertEquals(0, countDownNone.getCount());
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
            if (evt != EventType.CONFIGURATION_INITIALIZED) return; // if not the real event we skip the assert
            countDownLatch.countDown();
            Assert.assertNull("newKey", conf.getString(newKey));
        });
        configurationLifecycle.addConfigurationLifecycleListener(EventType.PROPERTY_ADDED, (evt, conf) -> {
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

    @Test
    public void testOnShutdownInvocationAfterSimpleStop() throws Exception {
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
        configurationLifecycle.stop();
        configurationLifecycle.stop(true);
        countDownLatch.await(30, TimeUnit.SECONDS);
        Assert.assertEquals(0, countDownLatch.getCount());
        assertEnvVars();
    }

    @Test
    public void testUnknownFile() throws Exception {
        this.configurationLifecycle.stop(true);
        String noopFileName = FILE.getAbsolutePath().replace("test.properties", UUID.randomUUID().toString() + ".properties");
        this.configurationLifecycle = new ConfigurationLifecycle(TRIGGER_PERIOD_MILLIS, noopFileName, true, NullLog.getInstance(), true);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        this.configurationLifecycle.addConfigurationLifecycleListener(EventType.CONFIGURATION_INITIALIZED, (evt, conf) -> {
            countDownLatch.countDown();
        });
        this.configurationLifecycle.start();
        countDownLatch.await(30, TimeUnit.SECONDS);
        Assert.assertEquals(0, countDownLatch.getCount());
        assertEnvVars();
    }

    @Test
    public void testSetPropertiesAPIs() throws Exception {
        this.configurationLifecycle.stop(true);
        String noopFileName = FILE.getAbsolutePath().replace("test.properties", UUID.randomUUID().toString() + ".properties");
        this.configurationLifecycle = new ConfigurationLifecycle(TRIGGER_PERIOD_MILLIS, noopFileName, true, NullLog.getInstance(), true);
        AtomicInteger counterAdded = new AtomicInteger();
        this.configurationLifecycle.addConfigurationLifecycleListener(EventType.PROPERTY_ADDED, (evt, conf) -> {
            counterAdded.incrementAndGet();
        });
        this.configurationLifecycle.start();
        Thread.sleep(2000);
        // this will trigger the following tree
        // "PROPERTY_ADDED" -> "CONFIGURATION_CHANGED" -> "CONFIGURATION_INITIALIZED"
        // this means that "CONFIGURATION_INITIALIZED" will triggered twice
        Map<String, Object> newProperties = new HashMap<>();
        newProperties.put(String.format("my.prop.%s", UUID.randomUUID().toString()), UUID.randomUUID().toString());
        newProperties.put(String.format("my.prop.%s", UUID.randomUUID().toString()), UUID.randomUUID().toString());
        configurationLifecycle.setProperties(newProperties);
        // we wait 10 seconds in order to be sure that the
        // listener gets invoked just once
        Thread.sleep(10000);
        Assert.assertEquals(1, counterAdded.get());
        // check if the file contains the new properties
        try (FileInputStream fileInputStream = new FileInputStream(noopFileName)) {
            Properties props = new Properties();
            props.load(fileInputStream);
            newProperties.forEach((key, value) -> Assert.assertEquals(value, props.getProperty(key)));
        }
        configurationLifecycle.stop();
        assertEnvVars();
    }

    @Test
    public void testSetPropertiesAPIsSaveFalse() throws Exception {
        AtomicInteger counterAdded = new AtomicInteger();
        configurationLifecycle.addConfigurationLifecycleListener(EventType.PROPERTY_ADDED, (evt, conf) -> {
            counterAdded.incrementAndGet();
        });
        Thread.sleep(2000);
        Map<String, Object> newProperties = new HashMap<>();
        newProperties.put(String.format("my.prop.%s", UUID.randomUUID().toString()), UUID.randomUUID().toString());
        newProperties.put(String.format("my.prop.%s", UUID.randomUUID().toString()), UUID.randomUUID().toString());
        configurationLifecycle.setProperties(newProperties, false);
        // we wait 10 seconds in order to be sure that the
        // listener gets invoked just once
        Thread.sleep(10000);
        Assert.assertEquals(1, counterAdded.get());
        // the file must not contain the new properties
        try (FileInputStream fileInputStream = new FileInputStream(FILE)) {
            Properties props = new Properties();
            props.load(fileInputStream);
            newProperties.forEach((key, value) -> Assert.assertNull(props.getProperty(key)));
        }
        configurationLifecycle.stop();
        assertEnvVars();
    }

    @Test
    public void testClearAPIs() throws Exception {
        long timestamp = System.currentTimeMillis();
        String newKey = String.format("my.prop.%d", timestamp);
        String newValue = UUID.randomUUID().toString();
        CountDownLatch countDownLatch = new CountDownLatch(3);
        configurationLifecycle.addConfigurationLifecycleListener(EventType.CONFIGURATION_INITIALIZED, (evt, conf) -> {
            if (evt != EventType.CONFIGURATION_INITIALIZED) return; // if not the real event we skip the assert
            countDownLatch.countDown();
            Assert.assertNull("newKey", conf.getString(newKey));
        });
        configurationLifecycle.addConfigurationLifecycleListener(EventType.PROPERTY_ADDED, (evt, conf) -> {
            countDownLatch.countDown();
            Assert.assertEquals(newValue, conf.getString(newKey));
        });
        configurationLifecycle.addConfigurationLifecycleListener(EventType.PROPERTY_REMOVED, (evt, conf) -> {
            countDownLatch.countDown();
            Assert.assertNull("newKey", conf.getString(newKey));
        });
        configurationLifecycle.start();
        Thread.sleep(2000);
        // this will trigger the following tree
        // "PROPERTY_ADDED" -> "CONFIGURATION_CHANGED" -> "CONFIGURATION_INITIALIZED"
        // this means that "CONFIGURATION_INITIALIZED" will triggered twice
        configurationLifecycle.setProperty(newKey, newValue);
        Thread.sleep(2000);
        // this will trigger the following tree
        // "PROPERTY_REMOVED" -> "CONFIGURATION_CHANGED" -> "CONFIGURATION_INITIALIZED"
        // this means that "CONFIGURATION_INITIALIZED" will triggered again
        configurationLifecycle.removeProperty(newKey);
        Assert.assertFalse(configurationLifecycle.getConfiguration().containsKey(newKey));
        countDownLatch.await(30, TimeUnit.SECONDS);
        Assert.assertEquals(0, countDownLatch.getCount());
        configurationLifecycle.stop();
        assertEnvVars();
    }

    @Test
    public void testClearAPIsSaveFalse() throws Exception {
        long timestamp = System.currentTimeMillis();
        String newKey = String.format("my.prop.%d", timestamp);
        String newValue = UUID.randomUUID().toString();
        CountDownLatch countDownLatch = new CountDownLatch(3);
        configurationLifecycle.addConfigurationLifecycleListener(EventType.CONFIGURATION_INITIALIZED, (evt, conf) -> {
            if (evt != EventType.CONFIGURATION_INITIALIZED) return; // if not the real event we skip the assert
            countDownLatch.countDown();
            Assert.assertNull("newKey", conf.getString(newKey));
        });
        configurationLifecycle.addConfigurationLifecycleListener(EventType.PROPERTY_ADDED, (evt, conf) -> {
            countDownLatch.countDown();
            Assert.assertEquals(newValue, conf.getString(newKey));
        });
        configurationLifecycle.addConfigurationLifecycleListener(EventType.PROPERTY_REMOVED, (evt, conf) -> {
            countDownLatch.countDown();
            Assert.assertNull("newKey", conf.getString(newKey));
        });
        configurationLifecycle.start();
        Thread.sleep(2000);
        // this will trigger the following tree
        // "PROPERTY_ADDED" -> "CONFIGURATION_CHANGED" -> "CONFIGURATION_INITIALIZED"
        // this means that "CONFIGURATION_INITIALIZED" will triggered twice
        configurationLifecycle.setProperty(newKey, newValue);
        Thread.sleep(2000);
        // this will trigger the following tree
        // "PROPERTY_REMOVED" -> "CONFIGURATION_CHANGED" -> "CONFIGURATION_INITIALIZED"
        // this means that "CONFIGURATION_INITIALIZED" will triggered again
        configurationLifecycle.removeProperty(newKey, false);
        Assert.assertFalse(configurationLifecycle.getConfiguration().containsKey(newKey));
        countDownLatch.await(30, TimeUnit.SECONDS);
        Assert.assertEquals(0, countDownLatch.getCount());
        configurationLifecycle.stop();
        try (FileInputStream fileInputStream = new FileInputStream(FILE)) {
            Properties props = new Properties();
            props.load(fileInputStream);
            Assert.assertEquals(newValue, props.getProperty(newKey));
        }
        assertEnvVars();
    }

    @Test
    public void testClearPropertiesAPIs() throws Exception {
        this.configurationLifecycle.stop(true);
        String noopFileName = FILE.getAbsolutePath().replace("test.properties", UUID.randomUUID().toString() + ".properties");
        this.configurationLifecycle = new ConfigurationLifecycle(TRIGGER_PERIOD_MILLIS, noopFileName, true, NullLog.getInstance(), true);
        AtomicInteger counterAdded = new AtomicInteger();
        this.configurationLifecycle.addConfigurationLifecycleListener(EventType.PROPERTY_ADDED, (evt, conf) -> {
            counterAdded.incrementAndGet();
        });
        AtomicInteger counterRemoved = new AtomicInteger();
        this.configurationLifecycle.addConfigurationLifecycleListener(EventType.PROPERTY_REMOVED, (evt, conf) -> {
            counterRemoved.incrementAndGet();
        });
        this.configurationLifecycle.start();
        Thread.sleep(2000);
        // this will trigger the following tree
        // "PROPERTY_ADDED" -> "CONFIGURATION_CHANGED" -> "CONFIGURATION_INITIALIZED"
        // this means that "CONFIGURATION_INITIALIZED" will triggered twice
        Map<String, Object> newProperties = new HashMap<>();
        newProperties.put(String.format("my.prop.%s", UUID.randomUUID().toString()), UUID.randomUUID().toString());
        newProperties.put(String.format("my.prop.%s", UUID.randomUUID().toString()), UUID.randomUUID().toString());
        configurationLifecycle.setProperties(newProperties);
        // we wait 10 seconds in order to be sure that the
        // listener gets invoked just once
        Thread.sleep(10000);
        Assert.assertEquals(1, counterAdded.get());
        configurationLifecycle.removeProperties(newProperties.keySet());
        // we wait 10 seconds in order to be sure that the
        // listener gets invoked just once
        Thread.sleep(10000);
        Assert.assertEquals(1, counterRemoved.get());
        configurationLifecycle.stop();
        assertEnvVars();
    }

    @Test
    public void testClearPropertiesAPIsSaveFalse() throws Exception {
        this.configurationLifecycle.stop(true);
        String noopFileName = FILE.getAbsolutePath().replace("test.properties", UUID.randomUUID().toString() + ".properties");
        this.configurationLifecycle = new ConfigurationLifecycle(TRIGGER_PERIOD_MILLIS, noopFileName, true, NullLog.getInstance(), true);
        AtomicInteger counterAdded = new AtomicInteger();
        this.configurationLifecycle.addConfigurationLifecycleListener(EventType.PROPERTY_ADDED, (evt, conf) -> {
            counterAdded.incrementAndGet();
        });
        AtomicInteger counterRemoved = new AtomicInteger();
        this.configurationLifecycle.addConfigurationLifecycleListener(EventType.PROPERTY_REMOVED, (evt, conf) -> {
            counterRemoved.incrementAndGet();
        });
        this.configurationLifecycle.start();
        Thread.sleep(2000);
        // this will trigger the following tree
        // "PROPERTY_ADDED" -> "CONFIGURATION_CHANGED" -> "CONFIGURATION_INITIALIZED"
        // this means that "CONFIGURATION_INITIALIZED" will triggered twice
        Map<String, Object> newProperties = new HashMap<>();
        newProperties.put(String.format("my.prop.%s", UUID.randomUUID().toString()), UUID.randomUUID().toString());
        newProperties.put(String.format("my.prop.%s", UUID.randomUUID().toString()), UUID.randomUUID().toString());
        configurationLifecycle.setProperties(newProperties);
        // we wait 10 seconds in order to be sure that the
        // listener gets invoked just once
        Thread.sleep(10000);
        Assert.assertEquals(1, counterAdded.get());
        configurationLifecycle.removeProperties(newProperties.keySet(), false);
        // we wait 10 seconds in order to be sure that the
        // listener gets invoked just once
        Thread.sleep(10000);
        Assert.assertEquals(1, counterRemoved.get());
        configurationLifecycle.stop();
        try (FileInputStream fileInputStream = new FileInputStream(noopFileName)) {
            Properties props = new Properties();
            props.load(fileInputStream);
            newProperties.forEach((k, v) -> Assert.assertEquals(v, props.getProperty(k)));
        }
        assertEnvVars();
    }
}
