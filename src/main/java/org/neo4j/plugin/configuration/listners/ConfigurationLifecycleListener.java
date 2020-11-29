package org.neo4j.plugin.configuration.listners;

import org.apache.commons.configuration2.ImmutableConfiguration;
import org.neo4j.plugin.configuration.EventType;

public interface ConfigurationLifecycleListener extends ShutdownListener {
    void onConfigurationChange(EventType event, ImmutableConfiguration config);

    @Override
    default void onShutdown(){}
}
