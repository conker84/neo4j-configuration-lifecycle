package org.neo4j.plugin.configuration;

import org.junit.Assert;
import org.junit.Test;

public class ConfigurationLifecycleUtilsTest {

    @Test
    public void testConvertEnvKey() {
        String key = "NEO4J_my__custom_var_with__dots_and__underscores";
        Assert.assertEquals("my_custom.var.with_dots.and_underscores",
                ConfigurationLifecycleUtils.convertEnvKey(key));
    }
}
