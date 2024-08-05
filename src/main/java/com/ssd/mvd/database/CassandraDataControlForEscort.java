package com.ssd.mvd.database;

import com.ssd.mvd.database.cassandraRegistry.CassandraConverter;
import com.ssd.mvd.interfaces.EntityToCassandraConverter;
import com.ssd.mvd.interfaces.DatabaseCommonMethods;
import com.ssd.mvd.interfaces.ServiceCommonMethods;
import com.ssd.mvd.constants.CassandraFunctions;
import com.ssd.mvd.inspectors.EntitiesInstances;
import com.ssd.mvd.constants.CassandraCommands;
import com.ssd.mvd.constants.CassandraTables;
import com.ssd.mvd.kafka.KafkaDataControl;
import com.ssd.mvd.entity.*;

import reactor.core.scheduler.Schedulers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.text.MessageFormat;
import java.util.function.*;
import java.util.Optional;
import java.util.List;

public final class CassandraDataControlForEscort
        extends CassandraConverter
        implements DatabaseCommonMethods, ServiceCommonMethods {
    private static CassandraDataControlForEscort cassandraDataControl = new CassandraDataControlForEscort();

    public static CassandraDataControlForEscort getInstance () {
        return cassandraDataControl != null
                ? cassandraDataControl
                : ( cassandraDataControl = new CassandraDataControlForEscort() );
    }

    private CassandraDataControlForEscort () {
        super.logging( this );
    }

    /*
        после получения сигнала от трекера обновляем его значения в БД
    */
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

                        super.joinWithAstrix( trackerInfo.getTrackerId() ),
                        super.joinWithAstrix( trackerInfo.getPatrulPassportSeries() ),
                        super.joinWithAstrix( trackerInfo.getGosNumber() ),
                        super.joinWithAstrix( trackerInfo.getStatus() ),

                        trackerInfo.getLatitude(),
                        trackerInfo.getLongitude(),

                        trackerInfo.getTotalActivityTime(),
                        CassandraFunctions.TO_TIMESTAMP.formatted( CassandraFunctions.NOW ),
                        super.joinWithAstrix( trackerInfo.getDateOfRegistration() )
                )
        );

        return trackerInfo;
    };

    public final Function< TupleOfCar, Mono< ApiResponseModel > > updateEscortCar = tupleOfCar ->
            super.convert(
                    this.findRowAndReturnEntity(
                            EntitiesInstances.TUPLE_OF_CAR,
                            "uuid",
                            tupleOfCar.getUuid().toString()
                    )
            ).flatMap( tupleOfCar1 -> {
                    final Optional< TupleOfCar > optional = Optional.of( tupleOfCar );

                    if (
                            optional.filter( tupleOfCar2 -> !tupleOfCar1.getTrackerId().equals( tupleOfCar.getTrackerId() )
                                    && !super.check( tupleOfCar.getTrackerId() ) ).isPresent()
                    ) {
                        return super.getResponse( super.getMap( "Wrong TrackerId" ) );
                    }

                    final StringBuilder stringBuilder = super.newStringBuilder();

                    optional.filter(
                            tupleOfCar2 -> super.objectIsNotNull( tupleOfCar1.getUuidOfPatrul() )
                                    && super.objectIsNotNull( tupleOfCar.getUuidOfPatrul() )
                                    && tupleOfCar1.getUuidOfPatrul().compareTo( tupleOfCar.getUuidOfPatrul() ) != 0
                    ).map( tupleOfCar2 -> {
                        stringBuilder.append(
                                MessageFormat.format(
                                        """
                                        {0} {1}.{2}
                                        SET uuidforescortcar = {3}
                                        WHERE uuid = {4};
                                        """,
                                        CassandraCommands.UPDATE,

                                        EntitiesInstances.PATRUL.getEntityKeyspaceName(),
                                        EntitiesInstances.PATRUL.getEntityTableName(),

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

                                        EntitiesInstances.PATRUL.getEntityKeyspaceName(),
                                        EntitiesInstances.PATRUL.getEntityTableName(),

                                        null,
                                        tupleOfCar1.getUuidOfPatrul()
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
                            EntitiesInstances.TUPLE_OF_CAR,
                            "uuid",
                            uuid
                    )
            ).flatMap( tupleOfCar1 -> !super.objectIsNotNull( tupleOfCar1.getUuidOfPatrul() )
                    && !super.objectIsNotNull( tupleOfCar1.getUuidOfEscort() )
                    ? super.getResponse(
                            super.getMap(
                                    uuid + " was removed successfully",
                                    tupleOfCar1.delete()
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
                                            EntitiesInstances.PATRUL,
                                            tupleOfCar.getUuidOfPatrul().toString(),
                                            "uuid"
                                    )
                            ).flatMap( patrul -> {
                                /*
                                соединяем патрульного с ID эскорт машины
                                 */
                                patrul.linkWithTupleOfCar( tupleOfCar );

                                patrul.updateEntity();

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
                    EntitiesInstances.TRACKER_INFO,
                    "trackersId",
                    super.joinWithAstrix( trackerId )
            )
    ).map( row -> {
            final TupleOfCar tupleOfCar = this.findRowAndReturnEntity(
                    EntitiesInstances.TUPLE_OF_CAR,
                    "gosNumber",
                    row.getString( "gosnumber" )
            );

            return super.objectIsNotNull( tupleOfCar.getUuidOfPatrul() )
                    ? new TrackerInfo(
                            this.findRowAndReturnEntity(
                                    EntitiesInstances.PATRUL,
                                    "uuid",
                                    tupleOfCar.getUuidOfPatrul().toString()
                            ),
                            tupleOfCar,
                            row
                    )
                    : new TrackerInfo( tupleOfCar, row );
        } );

    public final Supplier< Flux< TrackerInfo > > getAllTrackers = () -> CassandraDataControl
            .getInstance()
            .getAllEntities
            .apply( new EntityToCassandraConverter() {
                @Override
                public CassandraTables getEntityTableName() {
                    return CassandraTables.TRACKERSID;
                }

                @Override
                public CassandraTables getEntityKeyspaceName() {
                    return CassandraTables.ESCORT;
                }
            } )
            .map( row -> {
                final TupleOfCar tupleOfCar = this.findRowAndReturnEntity(
                        EntitiesInstances.TUPLE_OF_CAR,
                        "gosNumber",
                        super.joinWithAstrix( row.getString( "gosnumber" ) )
                );

                return super.objectIsNotNull( tupleOfCar.getUuidOfPatrul() )
                        ? new TrackerInfo(
                                this.findRowAndReturnEntity(
                                        EntitiesInstances.PATRUL,
                                        "uuid",
                                        tupleOfCar.getUuidOfPatrul().toString()
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
                        EntitiesInstances.TUPLE_OF_CAR,
                        row -> super.calculate( point, row ) <= point.getRadius()
                );

    public final Function< List< Point >, Flux< TupleOfCar > > findTheClosestCarsInPolygon = point ->
            CassandraDataControl
                .getInstance()
                .getConvertedEntities(
                        EntitiesInstances.TUPLE_OF_CAR,
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
