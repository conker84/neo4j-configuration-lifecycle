package org.neo4j.plugin.configuration;

import java.util.ArrayList;
import java.util.List;

public enum EventType {
    NONE(null),
    CONFIGURATION_INITIALIZED(null),
    CONFIGURATION_CHANGED(CONFIGURATION_INITIALIZED),
    PROPERTY_VALUE_CHANGED(CONFIGURATION_CHANGED),
    PROPERTY_ADDED(CONFIGURATION_CHANGED),
    PROPERTY_REMOVED(CONFIGURATION_CHANGED);

    private final EventType superType;

    EventType(EventType superType) {
        this.superType = superType;
    }

    public EventType getSuperType() {
        return superType;
    }

    public List<EventType> getTree() {
        List<EventType> tree = new ArrayList<>();
        tree.add(this);
        EventType evt = this;
        while ((evt = evt.getSuperType()) != null) {
            tree.add(evt);
        }
        return tree;
    }
}
