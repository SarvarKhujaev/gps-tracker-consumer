package com.ssd.mvd.inspectors;

import org.apache.commons.collections4.list.UnmodifiableList;
import com.ssd.mvd.annotations.EntityConstructorAnnotation;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.*;

import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@com.ssd.mvd.annotations.ImmutableEntityAnnotation
public class CollectionsInspector extends StringOperations {
    protected CollectionsInspector () {
        super( CollectionsInspector.class );
    }

    @EntityConstructorAnnotation( permission = TimeInspector.class )
    protected <T extends UuidInspector> CollectionsInspector ( @lombok.NonNull final Class<T> instance ) {
        super( CollectionsInspector.class );

        AnnotationInspector.checkCallerPermission( instance, CollectionsInspector.class );
        AnnotationInspector.checkAnnotationIsImmutable( CollectionsInspector.class );
    }

    @lombok.NonNull
    @lombok.Synchronized
    protected final synchronized <T, V> WeakHashMap<T, V> newMap () {
        return new WeakHashMap<>( 1 );
    }

    @lombok.NonNull
    @lombok.Synchronized
    protected static synchronized <T> List<T> emptyList () {
        return UnmodifiableList.unmodifiableList( Collections.EMPTY_LIST );
    }

    @lombok.NonNull
    @lombok.Synchronized
    public static synchronized <T> CopyOnWriteArrayList<T> newList () {
        return new CopyOnWriteArrayList<>();
    }

    @lombok.NonNull
    @lombok.Synchronized
    protected final synchronized <T> ArrayList<T> newList ( final int listSize ) {
        return new ArrayList<>( listSize );
    }

    @SafeVarargs
    @lombok.NonNull
    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_ -> !null" )
    public static synchronized <T> List<T> newList (
            @lombok.NonNull @com.typesafe.config.Optional final T ... objects
    ) {
        return UnmodifiableList.unmodifiableList( List.of( objects ) );
    }

    @lombok.NonNull
    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_, _ -> !null" )
    protected final synchronized <T> List<T> newList (
            final int duplicatesNumber,
            @lombok.NonNull @com.typesafe.config.Optional final T object
    ) {
        return IntStream
                .range( 0, duplicatesNumber )
                .mapToObj( value -> object )
                .collect( Collectors.toList() );
    }

    @lombok.NonNull
    @lombok.Synchronized
    protected final synchronized <T, V> TreeMap<T, V> newTreeMap () {
        return new TreeMap<>( Comparator.comparing( Objects::nonNull ) );
    }

    @lombok.Synchronized
    public static synchronized <T> void analyze (
            final Collection< T > someList,
            final Consumer< T > someConsumer
    ) {
        someList.forEach( someConsumer );
    }

    @lombok.Synchronized
    protected synchronized static <T> void analyze (
            @lombok.NonNull final Stream<T> someList,
            @lombok.NonNull final Consumer<T> someConsumer
    ) {
        someList.forEach( someConsumer );
    }

    @lombok.Synchronized
    protected final synchronized <T> boolean isCollectionNotEmpty ( final Collection<T> collection ) {
        return collection != null && !collection.isEmpty();
    }

    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_ -> true" )
    protected final synchronized <T, U> boolean isCollectionNotEmpty (
            final Map<T, U> map
    ) {
        return map != null && !map.isEmpty();
    }

    @lombok.NonNull
    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_ -> _" )
    protected static synchronized <T> List<T> convertArrayToList (
            @lombok.NonNull final T[] objects
    ) {
        return Arrays.asList( objects );
    }

    @lombok.NonNull
    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_ -> _" )
    protected static synchronized <T> Stream<T> convertArrayToStream (
            @lombok.NonNull final T[] objects
    ) {
        return Stream.of( objects );
    }

    @lombok.Synchronized
    protected final synchronized <T> void checkAndClear (
            final Collection<T> collection
    ) {
        if ( this.isCollectionNotEmpty( collection ) ) collection.clear();
    }

    @lombok.Synchronized
    protected final synchronized <T, U> void checkAndClear (
            final Map<T, U> map
    ) {
        if ( this.isCollectionNotEmpty( map ) ) map.clear();
    }

    @lombok.NonNull
    @lombok.Synchronized
    protected final synchronized Map< String, Object > getMap (
            final String key
    ) {
        return Map.of( "message", key );
    }

    @lombok.NonNull
    @lombok.Synchronized
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
