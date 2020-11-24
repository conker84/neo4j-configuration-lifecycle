package org.neo4j.plugin.configuration;

import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.ImmutableConfiguration;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ConfigurationLifecycleUtils {

    private ConfigurationLifecycleUtils() {}

    public static Set<String> configKeys(ImmutableConfiguration config) {
        return StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(config.getKeys(), Spliterator.SIZED), false)
                .collect(Collectors.toSet());
    }

    public static EventType getEventType(ImmutableConfiguration newConfig, ImmutableConfiguration oldConfig) {
        if (oldConfig == null) {
            return EventType.CONFIGURATION_INITIALIZED;
        }
        final Set<String> newFields = configKeys(newConfig);
        final Set<String> oldFields = configKeys(oldConfig);
        if (newConfig.size() > oldConfig.size() && newFields.containsAll(oldFields)) {
            return EventType.PROPERTY_ADDED;
        }
        if (oldConfig.size() < newConfig.size() && oldFields.containsAll(newFields)) {
            return EventType.PROPERTY_REMOVED;
        }
        if (newConfig.size() == oldConfig.size() && newFields.containsAll(oldFields)) {
            boolean isValueChanged = newFields.stream()
                    .anyMatch(field -> !newConfig.getString(field, "")
                            .equals(oldConfig.getString(field, "")));
            return isValueChanged ? EventType.PROPERTY_VALUE_CHANGED : EventType.NONE;
        }
        return EventType.CONFIGURATION_CHANGED;
    }

    public static Map<String, Object> toMap(ImmutableConfiguration config) {
        return StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(config.getKeys(), Spliterator.SIZED), false)
                .map(key -> new AbstractMap.SimpleEntry<>(key, config.getProperty(key)))
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
    }
}
