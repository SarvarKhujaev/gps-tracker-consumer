package com.ssd.mvd.inspectors;

import java.util.function.Consumer;
import java.util.*;

public class CollectionsInspector extends StringOperations {
    protected CollectionsInspector() {}

    protected <T, V> Map<T, V> newMap () {
        return new HashMap<>();
    }

    protected final synchronized <T, V> TreeMap<T, V> newTreeMap () {
        return new TreeMap<>();
    }

    protected <T> void analyze (
            final Collection< T > someList,
            final Consumer< T > someConsumer
    ) {
        someList.forEach( someConsumer );
    }

    protected <T> boolean isCollectionNotEmpty ( final Collection<T> collection ) {
        return collection != null && !collection.isEmpty();
    }

    protected final synchronized <T> List<T> convertArrayToList (
            final T[] objects
    ) {
        return Arrays.asList( objects );
    }

    protected final synchronized Map< String, Object > getMap (
            final String key
    ) {
        return Map.of( "message", key );
    }

    protected final synchronized Map< String, Object > getMap (
            final String key,
            final boolean value
    ) {
        return Map.of(
                "message", key,
                "success", value
        );
    }
}
