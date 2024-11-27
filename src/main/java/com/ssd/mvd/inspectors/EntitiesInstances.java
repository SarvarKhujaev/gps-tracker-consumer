package com.ssd.mvd.inspectors;

import com.ssd.mvd.entity.patrulDataSet.patrulSubClasses.PatrulFuelStatistics;
import com.ssd.mvd.entity.patrulDataSet.Patrul;
import com.ssd.mvd.entity.*;

import com.ssd.mvd.inspectors.dataTypesInpectors.StringOperations;

import org.apache.commons.collections4.list.UnmodifiableList;
import org.apache.commons.lang3.Validate;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.concurrent.atomic.AtomicReference;
import java.lang.ref.WeakReference;
import java.util.List;

@SuppressWarnings( value = "хранит instance на все объекты" )
@com.ssd.mvd.annotations.services.ImmutableEntityAnnotation
public final class EntitiesInstances {
    @lombok.NonNull
    @lombok.Synchronized
    public static synchronized <T> WeakReference<T> generateWeakEntity (final T entity ) {
        Validate.notNull( entity, StringOperations.NULL_VALUE_IN_ASSERT );
        return new WeakReference<>( entity );
    }

    @lombok.NonNull
    @lombok.Synchronized
    public static synchronized <T> WeakReference<List<T>> generateWeakEntity () {
        return new WeakReference<>( CollectionsInspector.emptyList() );
    }

    @lombok.NonNull
    @lombok.Synchronized
    public static synchronized <T> AtomicReference<T> generateAtomicEntity (@lombok.NonNull final T entity ) {
        Validate.notNull( entity, StringOperations.NULL_VALUE_IN_ASSERT );
        return new AtomicReference<>( entity );
    }

    public static final AtomicReference< Icons > ICONS = generateAtomicEntity(
            new Icons()
    );
    public static final AtomicReference< Patrul > PATRUL = generateAtomicEntity(
            new Patrul()
    );
    public static final AtomicReference< ReqCar > REQ_CAR = generateAtomicEntity(
            new ReqCar()
    );
    public static final AtomicReference< PoliceType > POLICE_TYPE = generateAtomicEntity(
            new PoliceType()
    );
    public static final AtomicReference< TupleOfCar > TUPLE_OF_CAR = generateAtomicEntity(
            new TupleOfCar()
    );
    public static final AtomicReference< TrackerInfo > TRACKER_INFO = generateAtomicEntity(
            new TrackerInfo()
    );
    public static final AtomicReference< PositionInfo > POSITION_INFO = generateAtomicEntity(
            new PositionInfo()
    );
    public static final AtomicReference< PatrulFuelStatistics > PATRUL_FUEL_STATISTICS = generateAtomicEntity(
            new PatrulFuelStatistics()
    );

    public static final WeakReference< org.apache.kafka.common.serialization.StringSerializer > KAFKA_STRING_SERIALIZER = generateWeakEntity(
            new org.apache.kafka.common.serialization.StringSerializer()
    );

    public static final WeakReference< org.apache.kafka.common.serialization.ByteArraySerializer > KAFKA_BYTE_SERIALIZER = generateWeakEntity(
            new org.apache.kafka.common.serialization.ByteArraySerializer()
    );

    public final static WeakReference< Serde< String > > STRING_SERDE = generateWeakEntity( Serdes.String() );

    public static final UnmodifiableList< AtomicReference< ? > > instancesList = new UnmodifiableList<>(
            List.of(
                    ICONS,
                    PATRUL,
                    REQ_CAR,
                    POLICE_TYPE,
                    TUPLE_OF_CAR,
                    TRACKER_INFO,
                    POSITION_INFO,
                    PATRUL_FUEL_STATISTICS
            )
    );

    public static void clear() {
        STRING_SERDE.get().close();
        KAFKA_BYTE_SERIALIZER.get().close();
        KAFKA_STRING_SERIALIZER.get().close();

        CustomServiceCleaner.clearReference( KAFKA_BYTE_SERIALIZER );
        CustomServiceCleaner.clearReference( KAFKA_STRING_SERIALIZER );
    }
}
