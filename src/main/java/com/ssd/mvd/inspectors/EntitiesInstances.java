package com.ssd.mvd.inspectors;

import com.ssd.mvd.entity.patrulDataSet.patrulSubClasses.*;
import com.ssd.mvd.entity.patrulDataSet.Patrul;
import com.ssd.mvd.entity.*;

import com.ssd.mvd.inspectors.dataTypesInpectors.StringOperations;

import org.apache.commons.collections4.list.UnmodifiableList;
import org.apache.commons.lang3.Validate;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;

import java.util.concurrent.atomic.AtomicReference;
import java.lang.ref.WeakReference;
import java.util.List;

@SuppressWarnings( value = "хранит instance на все объекты" )
@com.ssd.mvd.annotations.services.ImmutableEntityAnnotation
public final class EntitiesInstances extends AnnotationInspector {
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
            checkAnnotationIsNotImmutable( new Icons( EntitiesInstances.class ) )
    );
    public static final AtomicReference< Patrul > PATRUL = generateAtomicEntity(
            checkAnnotationIsNotImmutable( new Patrul( EntitiesInstances.class ) )
    );
    public static final AtomicReference< Position > POSITION_ATOMIC_REFERENCE = generateAtomicEntity(
            checkAnnotationIsNotImmutable( new Position( EntitiesInstances.class ) )
    );
    public static final AtomicReference< ReqCar > REQ_CAR = generateAtomicEntity(
            checkAnnotationIsNotImmutable( new ReqCar( EntitiesInstances.class ) )
    );
    public static final AtomicReference< PoliceType > POLICE_TYPE = generateAtomicEntity(
            checkAnnotationIsNotImmutable( new PoliceType( EntitiesInstances.class ) )
    );
    public static final AtomicReference< TupleOfCar > TUPLE_OF_CAR = generateAtomicEntity(
            checkAnnotationIsNotImmutable( new TupleOfCar( EntitiesInstances.class ) )
    );
    public static final AtomicReference< TrackerInfo > TRACKER_INFO = generateAtomicEntity(
            checkAnnotationIsNotImmutable( new TrackerInfo() )
    );
    public static final AtomicReference< PositionInfo > POSITION_INFO = generateAtomicEntity(
            checkAnnotationIsNotImmutable( new PositionInfo( EntitiesInstances.class ) )
    );
    public static final AtomicReference< PatrulFuelStatistics > PATRUL_FUEL_STATISTICS = generateAtomicEntity(
            new PatrulFuelStatistics( EntitiesInstances.class )
    );

    @SuppressWarnings( value = "Patrul sub classess" )
    public static final AtomicReference< PatrulCarInfo > PATRUL_CAR_INFO = generateAtomicEntity(
            checkAnnotationIsSubClass(
                    checkAnnotationIsNotImmutable( new PatrulCarInfo( EntitiesInstances.class ) )
            )
    );
    public static final AtomicReference< PatrulFIOData > PATRUL_FIO_INFO = generateAtomicEntity(
            checkAnnotationIsSubClass(
                    checkAnnotationIsNotImmutable( new PatrulFIOData( EntitiesInstances.class ) )
            )
    );
    public static final AtomicReference< PatrulTaskInfo > PATRUL_TASK_INFO = generateAtomicEntity(
            checkAnnotationIsSubClass(
                    checkAnnotationIsNotImmutable( new PatrulTaskInfo( EntitiesInstances.class ) )
            )
    );
    public static final AtomicReference< PatrulRegionData > PATRUL_REGION_DATA = generateAtomicEntity(
            checkAnnotationIsSubClass(
                    checkAnnotationIsNotImmutable( new PatrulRegionData( EntitiesInstances.class ) )
            )
    );
    public static final AtomicReference< PatrulUniqueValues > PATRUL_UNIQUE_VALUES = generateAtomicEntity(
            checkAnnotationIsSubClass(
                    checkAnnotationIsNotImmutable( new PatrulUniqueValues( EntitiesInstances.class ) )
            )
    );
    public static final AtomicReference< PatrulLocationData > PATRUL_LOCATION_DATA = generateAtomicEntity(
            checkAnnotationIsSubClass(
                    checkAnnotationIsNotImmutable( new PatrulLocationData( EntitiesInstances.class ) )
            )
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
