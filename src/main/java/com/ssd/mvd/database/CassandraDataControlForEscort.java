package com.ssd.mvd.database;

import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.driver.core.BatchStatement;

import com.ssd.mvd.interfaces.DatabaseCommonMethods;

import com.ssd.mvd.inspectors.CassandraConverter;
import com.ssd.mvd.inspectors.EntitiesInstances;

import com.ssd.mvd.kafka.KafkaDataControl;
import com.ssd.mvd.entity.*;

import reactor.core.scheduler.Schedulers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.ref.WeakReference;

import java.util.function.*;
import java.util.Optional;
import java.util.List;

public final class CassandraDataControlForEscort extends CassandraConverter implements DatabaseCommonMethods {
    private static CassandraDataControlForEscort cassandraDataControl = new CassandraDataControlForEscort();

    @lombok.NonNull
    @lombok.Synchronized
    public static synchronized CassandraDataControlForEscort getInstance () {
        return cassandraDataControl != null
                ? cassandraDataControl
                : ( cassandraDataControl = new CassandraDataControlForEscort() );
    }

    private CassandraDataControlForEscort () {
        super( CassandraDataControlForEscort.class );
        super.logging( this );
    }

    @SuppressWarnings(
            value = "после получения сигнала от трекера обновляем его значения в БД"
    )
    private final Function< TrackerInfo, TrackerInfo > saveTackerInfo = trackerInfo -> {
        this.completeCommand(
                trackerInfo.startInsert()
                        .value(
                                CqlIdentifier.fromCql( "trackersId" ),
                                QueryBuilder.literal( trackerInfo.getTrackerId() )
                        ).value(
                                CqlIdentifier.fromCql( "patrulPassportSeries" ),
                                QueryBuilder.literal( trackerInfo.getPatrulPassportSeries() )
                        ).value(
                                CqlIdentifier.fromCql( "gosnumber" ),
                                QueryBuilder.literal( trackerInfo.getGosNumber() )
                        ).value(
                                CqlIdentifier.fromCql( "status" ),
                                QueryBuilder.literal( trackerInfo.getStatus() )
                        ).value(
                                CqlIdentifier.fromCql( "latitude" ),
                                QueryBuilder.literal( trackerInfo.getLatitude() )
                        ).value(
                                CqlIdentifier.fromCql( "longitude" ),
                                QueryBuilder.literal( trackerInfo.getLongitude() )
                        ).value(
                                CqlIdentifier.fromCql( "totalActivityTime" ),
                                QueryBuilder.literal( trackerInfo.getTotalActivityTime() )
                        ).value(
                                CqlIdentifier.fromCql( "lastActiveDate" ),
                                QueryBuilder.now()
                        ).value(
                                CqlIdentifier.fromCql( "dateOfRegistration" ),
                                QueryBuilder.literal( trackerInfo.getDateOfRegistration() )
                        )
        );

        return trackerInfo;
    };

    public final Function< TupleOfCar, Mono< ApiResponseModel > > updateEscortCar = tupleOfCar ->
            super.convert(
                    this.findRowAndReturnEntity(
                            EntitiesInstances.TUPLE_OF_CAR.get(),
                            tupleOfCar.getUuid().toString()
                    )
            ).flatMap( tupleOfCar1 -> {
                    final Optional< TupleOfCar > optional = getOptional( tupleOfCar );

                    if (
                            optional.filter(
                                    tupleOfCar2 -> !tupleOfCar1.get().getTrackerId().equals( tupleOfCar.getTrackerId() )
                                            && !super.check( tupleOfCar.getTrackerId() )
                            ).isPresent()
                    ) {
                        return super.getResponse( super.getMap( "Wrong TrackerId" ) );
                    }

                    final BatchStatement batchStatement = new BatchStatement();

                    optional.filter(
                            tupleOfCar2 -> super.objectIsNotNull( tupleOfCar1.get().getUuidOfPatrul() )
                                    && super.objectIsNotNull( tupleOfCar.getUuidOfPatrul() )
                                    && tupleOfCar1.get().getUuidOfPatrul().compareTo( tupleOfCar.getUuidOfPatrul() ) != 0
                    ).ifPresent( tupleOfCar2 -> batchStatement.add(
                            this.generatePreparedStatement(
                                    EntitiesInstances.PATRUL.get()
                                            .startUpdate()
                                            .set(
                                                    Assignment.setField(
                                                            CqlIdentifier.fromCql(
                                                                    getSubClassColumnName( EntitiesInstances.PATRUL_UNIQUE_VALUES.get() )
                                                            ),
                                                            CqlIdentifier.fromCql( "uuidForEscortCar" ),
                                                            QueryBuilder.bindMarker()
                                                    )
                                            ).where(
                                                    Relation.column(
                                                            getEntityPrimaryKey( EntitiesInstances.PATRUL.get() )[0]
                                                    ).isEqualTo( QueryBuilder.bindMarker() )
                                            )
                            ).bind(
                                    tupleOfCar.getUuid(),
                                    tupleOfCar.getUuidOfPatrul()
                            )
                    ).add(
                            this.generatePreparedStatement(
                                    EntitiesInstances.PATRUL.get()
                                            .startUpdate()
                                            .set(
                                                    Assignment.setField(
                                                            CqlIdentifier.fromCql(
                                                                    getSubClassColumnName( EntitiesInstances.PATRUL_UNIQUE_VALUES.get() )
                                                            ),
                                                            CqlIdentifier.fromCql( "uuidForEscortCar" ),
                                                            QueryBuilder.bindMarker()
                                                    )
                                            ).where(
                                                    Relation.column(
                                                            getEntityPrimaryKey( EntitiesInstances.PATRUL.get() )[0]
                                                    ).isEqualTo( QueryBuilder.bindMarker() )
                                            )
                            ).bind(
                                    null,
                                    tupleOfCar1.get().getUuidOfPatrul()
                            )
                    ) );

                    batchStatement.add(
                            this.generatePreparedStatement( tupleOfCar.getEntityInsert() ).bind()
                    );

                    return this.completeCommand( batchStatement ).wasApplied()
                            ? super.getResponse(
                                    super.getMap( "Car" + tupleOfCar.getGosNumber() + " was updated successfully" )
                            )
                            : super.getResponse(
                                    super.getMap(
                                            "This car does not exists",
                                            false
                                    )
                            );
                } );

    public final Function< String, Mono< ApiResponseModel > > deleteTupleOfCar = uuid ->
            super.convert(
                    this.findRowAndReturnEntity(
                            EntitiesInstances.TUPLE_OF_CAR.get(),
                            uuid
                    )
            ).flatMap( tupleOfCar1 -> !super.objectIsNotNull( tupleOfCar1.get().getUuidOfPatrul() )
                    && !super.objectIsNotNull( tupleOfCar1.get().getUuidOfEscort() )
                    ? super.getResponse(
                            super.getMap(
                                    uuid + " was removed successfully",
                                    tupleOfCar1.get().delete()
                            )
                    )
                    : super.getResponse(
                            super.getMap(
                                    "You cannot delete this car, it is linked to Patrul or Escort",
                                    false
                            )
                    )
            );

    public final Function< TupleOfCar, Mono< ApiResponseModel > > saveNewTupleOfCar = tupleOfCar ->
            super.check( tupleOfCar.getTrackerId() )
            && super.checkCarNumber( tupleOfCar.getGosNumber() )
                    ? tupleOfCar.save()
                    /*
                    проверяем что Эскорт сявзан с каким-либо патрульным
                    */
                    ? super.objectIsNotNull( tupleOfCar.getUuidOfPatrul() )
                            /*
                            если да, то находим патрульного и связываем его с эскортом
                            */
                            ? super.convert(
                                    this.findRowAndReturnEntity(
                                            EntitiesInstances.PATRUL.get(),
                                            tupleOfCar.getUuidOfPatrul().toString()
                                    )
                            ).flatMap( patrul -> {
                                /*
                                соединяем патрульного с ID эскорт машины
                                */
                                patrul.get().linkWithTupleOfCar( tupleOfCar );

                                patrul.get().updateEntity();

                                /*
                                отправляем уведомлнеие через Кафку,
                                о том что новый эскорт был добален в БД
                                */
                                KafkaDataControl
                                        .getKafkaDataControl()
                                        .sendMessageToKafka( tupleOfCar );

                                /*
                                сохраняем в статичный кэш
                                */
                                tupleOfCarMap.putIfAbsent(
                                        tupleOfCar.getTrackerId(),
                                        this.saveTackerInfo.apply( new TrackerInfo( patrul, tupleOfCar ) )
                                );

                                return super.getResponse(
                                        super.getMap(
                                                "Escort was saved successfully",
                                                true
                                        )
                                );
                            } )
                            : super.getResponse(
                                    super.getMap(
                                            "Escort was saved successfully"
                                            + tupleOfCarMap.putIfAbsent(
                                                    tupleOfCar.getTrackerId(),
                                                    this.saveTackerInfo.apply( new TrackerInfo( tupleOfCar ) )
                                            ).getTrackerId()
                                    )
                            )
                    : super.getResponse( super.getMap( "This car is already exists" ) )
            : super.getResponse(
                    super.getMap(
                            "This trackers or gosnumber is already registered to another car, so choose another one"
                    )
            );

    public final Function< String, Mono< TrackerInfo > > getCurrentTracker = trackerId -> super.convert(
            this.getRowFromTabletsKeyspace(
                    EntitiesInstances.TRACKER_INFO.get(),
                    "trackersId",
                    trackerId
            )
    ).map( row -> {
            final WeakReference< TupleOfCar > tupleOfCar = this.findRowAndReturnEntity(
                    EntitiesInstances.TUPLE_OF_CAR.get(),
                    "gosNumber",
                    row.get().getString( "gosnumber" )
            );

            return super.objectIsNotNull( tupleOfCar.get().getUuidOfPatrul() )
                    ? new TrackerInfo(
                            this.findRowAndReturnEntity(
                                    EntitiesInstances.PATRUL.get(),
                                    tupleOfCar.get().getUuidOfPatrul()
                            ),
                            tupleOfCar,
                            row.get()
                    )
                    : new TrackerInfo( tupleOfCar, row.get() );
        } );

    public final Supplier< Flux< TrackerInfo > > getAllTrackers = () -> CassandraDataControl
            .getInstance()
            .getAllEntities
            .apply( EntitiesInstances.TRACKER_INFO.get() )
            .map( row -> {
                final WeakReference< TupleOfCar > tupleOfCar = this.findRowAndReturnEntity(
                        EntitiesInstances.TUPLE_OF_CAR.get(),
                        "gosNumber",
                        row.getString( "gosnumber" )
                );

                return super.objectIsNotNull( tupleOfCar.get().getUuidOfPatrul() )
                        ? new TrackerInfo(
                                this.findRowAndReturnEntity(
                                        EntitiesInstances.PATRUL.get(),
                                        tupleOfCar.get().getUuidOfPatrul().toString()
                                ),
                                tupleOfCar,
                                row
                        )
                        : new TrackerInfo( tupleOfCar, row );
            } )
            .sequential()
            .publishOn( Schedulers.single() );

    public final Function< Point, Flux< TupleOfCar > > findTheClosestCarsInRadius = point ->
            CassandraDataControl
                    .getInstance()
                    .getConvertedEntities(
                            EntitiesInstances.TUPLE_OF_CAR.get(),
                            row -> super.calculate( point, row ) <= point.getRadius()
                    );

    public final Function< List< Point >, Flux< TupleOfCar > > findTheClosestCarsInPolygon = point ->
            CassandraDataControl
                .getInstance()
                .getConvertedEntities(
                        EntitiesInstances.TUPLE_OF_CAR.get(),
                        row -> super.calculateDistanceInSquare( point, row )
                );

    @Override
    public void close() {
        CassandraDataControl.getInstance().close();
        cassandraDataControl = null;
        super.logging( this );
        this.clean();
    }
}
