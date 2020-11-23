package org.neo4j.plugin.configuration;

import org.apache.commons.configuration2.ImmutableConfiguration;

public interface ConfigurationLifecycleListener {
    void onConfigurationChange(String event, ImmutableConfiguration config);
}
