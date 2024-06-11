package com.ssd.mvd.inspectors;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.*;

public class CollectionsInspector extends StringOperations {
    protected CollectionsInspector() {}

    protected <T, V> Map<T, V> newMap () {
        return new HashMap<>();
    }

    protected <T> void analyze (
            final Collection< T > someList,
            final Consumer<T> someConsumer
    ) {
        someList.forEach( someConsumer );
    }

    protected Stream< Row > convertRowToStream ( final ResultSet resultSet ) {
        return resultSet.all().parallelStream();
    }

    protected <T> boolean isCollectionNotEmpty ( final Collection<T> collection ) {
        return collection != null && !collection.isEmpty();
    }

    protected final synchronized <T> List<T> convertArrayToList (
            final T[] objects
    ) {
        return Arrays.asList( objects );
    }
}
