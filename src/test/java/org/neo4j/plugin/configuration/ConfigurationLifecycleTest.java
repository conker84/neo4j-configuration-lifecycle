package org.neo4j.plugin.configuration;

import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ConfigurationLifecycleTest {

    @Test
    public void testReloadFile() throws Exception {
        long timestamp = System.currentTimeMillis();
        String newKey = String.format("my.prop.%d", timestamp);
        String newValue = UUID.randomUUID().toString();
        File file = new File(Thread.currentThread().getContextClassLoader().getResource("test.properties").toURI());
        ConfigurationLifecycle configurationLifecycle = new ConfigurationLifecycle(1, file.getAbsolutePath());
        CountDownLatch countDownLatch = new CountDownLatch(2);
        configurationLifecycle.addConfigurationLifecycleListener(EventType.CONFIGURATION_CHANGED, (evt, conf) -> {
            countDownLatch.countDown();
            int count = (int) countDownLatch.getCount();
            switch (count) {
                case 0:
                    Assert.assertEquals(newValue, conf.getString(newKey));
                    break;
                case 1:
                    Assert.assertNull("Should not contain newKey:" + newKey, conf.getString(newKey));
                    break;
            }
        });
        configurationLifecycle.start();
        try (FileWriter fw = new FileWriter(file, true);
             BufferedWriter bw = new BufferedWriter(fw)) {
            bw.write(String.format("%s=%s", newKey, newValue));
            bw.newLine();
        }
        countDownLatch.await(30, TimeUnit.SECONDS);
        Assert.assertEquals(0, countDownLatch.getCount());
    }

    @Test
    public void testReloadFileAddBlankLine() throws Exception {
        File file = new File(Thread.currentThread().getContextClassLoader().getResource("test.properties").toURI());
        ConfigurationLifecycle configurationLifecycle = new ConfigurationLifecycle(1, file.getAbsolutePath());
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
        try (FileWriter fw = new FileWriter(file, true);
             BufferedWriter bw = new BufferedWriter(fw)) {
            bw.write("\n\r");
            bw.newLine();
        }
        Thread.sleep(2000);
        Assert.assertEquals(0, countConfigurationChanged.get());
        Assert.assertEquals(0, countNone.get());
    }

    @Test
    public void testSetAPIs() throws Exception {
        long timestamp = System.currentTimeMillis();
        String newKey = String.format("my.prop.%d", timestamp);
        String newValue = UUID.randomUUID().toString();
        File file = new File(Thread.currentThread().getContextClassLoader().getResource("test.properties").toURI());
        ConfigurationLifecycle configurationLifecycle = new ConfigurationLifecycle(1, file.getAbsolutePath());
        CountDownLatch countDownLatch = new CountDownLatch(2);
        configurationLifecycle.addConfigurationLifecycleListener(EventType.CONFIGURATION_CHANGED, (evt, conf) -> {
            countDownLatch.countDown();
            System.out.println("evt = " + evt);
            System.out.println("conf = " + conf);
            int count = (int) countDownLatch.getCount();
            switch (count) {
                case 0:
                    Assert.assertEquals(newValue, conf.getString(newKey));
                    break;
                case 1:
                    Assert.assertNull("Should not contain newKey:" + newKey, conf.getString(newKey));
                    break;
            }
        });
        configurationLifecycle.start();
        configurationLifecycle.setProperty(newKey, newValue);
        countDownLatch.await(30, TimeUnit.SECONDS);
        Assert.assertEquals(0, countDownLatch.getCount());
    }

    @Test
    public void testSetAPIsShouldNotBeInvoked() throws Exception {
        File file = new File(Thread.currentThread().getContextClassLoader().getResource("test.properties").toURI());
        ConfigurationLifecycle configurationLifecycle = new ConfigurationLifecycle(1, file.getAbsolutePath());
        AtomicInteger countConfigurationChanged = new AtomicInteger();
        AtomicInteger countNone = new AtomicInteger();
        configurationLifecycle.addConfigurationLifecycleListener(EventType.CONFIGURATION_CHANGED, (evt, conf) -> {
            countConfigurationChanged.incrementAndGet();
        });
        configurationLifecycle.addConfigurationLifecycleListener(EventType.NONE, (evt, conf) -> {
            countNone.incrementAndGet();
        });
        configurationLifecycle.start();
        configurationLifecycle.setProperty("foo", "bar");
        Thread.sleep(2000);
        Assert.assertEquals(0, countConfigurationChanged.get());
        Assert.assertEquals(1, countNone.get());
    }
}
