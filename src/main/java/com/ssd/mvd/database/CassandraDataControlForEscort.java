package com.ssd.mvd.database;

import com.ssd.mvd.interfaces.EntityToCassandraConverter;
import com.ssd.mvd.interfaces.DatabaseCommonMethods;

import com.ssd.mvd.inspectors.CassandraConverter;
import com.ssd.mvd.inspectors.EntitiesInstances;

import com.ssd.mvd.kafka.KafkaDataControl;
import com.ssd.mvd.entity.*;

import com.ssd.mvd.constants.CassandraFunctions;
import com.ssd.mvd.constants.CassandraCommands;
import com.ssd.mvd.constants.CassandraTables;

import reactor.core.scheduler.Schedulers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.ref.WeakReference;
import java.text.MessageFormat;

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
                MessageFormat.format(
                        """
                        {0} {1}.{2}
                        (
                            trackersId,
                            patrulPassportSeries,
                            gosnumber,
                            status
                            latitude,
                            longitude,
                            totalActivityTime,
                            lastActiveDate,
                            dateOfRegistration
                        )
                        VALUES( {3}, {4}, {5}, {6}, {7}, {8}, {9,number,#}, {10}, {11} );
                        """,

                        CassandraCommands.INSERT_INTO,

                        CassandraTables.ESCORT,
                        CassandraTables.TRACKERSID,

                        joinWithAstrix( trackerInfo.getTrackerId() ),
                        joinWithAstrix( trackerInfo.getPatrulPassportSeries() ),
                        joinWithAstrix( trackerInfo.getGosNumber() ),
                        joinWithAstrix( trackerInfo.getStatus() ),

                        trackerInfo.getLatitude(),
                        trackerInfo.getLongitude(),

                        trackerInfo.getTotalActivityTime(),
                        CassandraFunctions.TO_TIMESTAMP.formatted( CassandraFunctions.NOW ),
                        joinWithAstrix( trackerInfo.getDateOfRegistration() )
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

                    final StringBuilder stringBuilder = super.newStringBuilder();

                    optional.filter(
                            tupleOfCar2 -> super.objectIsNotNull( tupleOfCar1.get().getUuidOfPatrul() )
                                    && super.objectIsNotNull( tupleOfCar.getUuidOfPatrul() )
                                    && tupleOfCar1.get().getUuidOfPatrul().compareTo( tupleOfCar.getUuidOfPatrul() ) != 0
                    ).map( tupleOfCar2 -> {
                        stringBuilder.append(
                                MessageFormat.format(
                                        """
                                        {0} {1}.{2}
                                        SET uuidforescortcar = {3}
                                        WHERE uuid = {4};
                                        """,
                                        CassandraCommands.UPDATE,

                                        EntitiesInstances.PATRUL.get().getEntityKeyspaceName(),
                                        EntitiesInstances.PATRUL.get().getEntityTableName(),

                                        tupleOfCar.getUuid(),
                                        tupleOfCar.getUuidOfPatrul()
                                )
                        ).append(
                                MessageFormat.format(
                                        """
                                        {0} {1}.{2}
                                        SET uuidforescortcar = {3}
                                        WHERE uuid = {4};
                                        """,
                                        CassandraCommands.UPDATE,

                                        EntitiesInstances.PATRUL.get().getEntityKeyspaceName(),
                                        EntitiesInstances.PATRUL.get().getEntityTableName(),

                                        null,
                                        tupleOfCar1.get().getUuidOfPatrul()
                                )
                        );

                        return tupleOfCar;
                    } );

                    stringBuilder.append(
                            tupleOfCar.getEntityInsertCommand()
                    ).append( CassandraCommands.APPLY_BATCH );

                    return this.completeCommand( stringBuilder.toString() ).wasApplied()
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
                                        .getInstance()
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
                    joinWithAstrix( trackerId )
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
            .apply( new EntityToCassandraConverter() {
                @Override
                @lombok.NonNull
                public CassandraTables getEntityTableName() {
                    return CassandraTables.TRACKERSID;
                }

                @Override
                @lombok.NonNull
                public CassandraTables getEntityKeyspaceName() {
                    return CassandraTables.ESCORT;
                }
            } )
            .map( row -> {
                final WeakReference< TupleOfCar > tupleOfCar = this.findRowAndReturnEntity(
                        EntitiesInstances.TUPLE_OF_CAR.get(),
                        "gosNumber",
                        joinWithAstrix( row.getString( "gosnumber" ) )
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
