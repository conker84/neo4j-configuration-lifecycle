package org.neo4j.plugin.configuration;

import org.apache.commons.configuration2.ImmutableConfiguration;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
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
        if (newConfig.size() > oldConfig.size() && (oldFields.isEmpty() || newFields.containsAll(oldFields))) {
            return EventType.PROPERTY_ADDED;
        }
        if (oldConfig.size() > newConfig.size() && (newFields.isEmpty() || oldFields.containsAll(newFields))) {
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

    public static String convertEnvKey(String key) {
        final String newKey = (key == null ? "" : key)
                .replace("NEO4J_", "");
        return Arrays.asList(newKey.split("__"))
                .stream()
                .map(s -> s.replace("_", "."))
                .collect(Collectors.joining("_"));
    }

    public static Map<String, Object> getNeo4jEnvVars(Collection<String> supportedEnvVarPrefixes) {
        return System.getenv()
                .entrySet()
                .stream()
                .filter(e -> e.getKey().startsWith("NEO4J_"))
                .map(e -> new AbstractMap.SimpleEntry<>(convertEnvKey(e.getKey()), e.getValue()))
                .filter(e -> supportedEnvVarPrefixes.isEmpty() ? true : supportedEnvVarPrefixes.stream().anyMatch(supportedPrefix -> e.getKey().startsWith(supportedPrefix)))
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
    }
}
