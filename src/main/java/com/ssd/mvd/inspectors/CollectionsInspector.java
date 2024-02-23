package com.ssd.mvd.inspectors;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.*;

public class CollectionsInspector extends StringOperations {
    protected CollectionsInspector() {}

    protected <T> List<T> emptyList () {
        return Collections.emptyList();
    }

    protected <T> ArrayList<T> newList () {
        return new ArrayList<>();
    }

    protected <T, V> Map<T, V> newMap () {
        return new HashMap<>();
    }

    protected boolean checkCollectionsLengthEquality (
            final Map firstCollection,
            final Collection secondCollection ) {
        return firstCollection.size() == secondCollection.size();
    }

    protected <T> void analyze (
            final Collection< T > someList,
            final Consumer<T> someConsumer ) {
        someList
                .parallelStream()
                .forEach( someConsumer );
    }

    protected <T, V> void analyze (
            final Map< T, V > someList,
            final BiConsumer<T, V> someConsumer ) {
        someList.forEach( someConsumer );
    }

    protected Stream< Row > convertRowToStream ( final ResultSet resultSet ) {
        return resultSet.all().parallelStream();
    }

    protected <T> boolean isCollectionNotEmpty ( final Collection<T> collection ) {
        return collection != null && !collection.isEmpty();
    }
}
