package com.ssd.mvd.database;

import com.ssd.mvd.database.cassandraRegistry.CassandraConverter;
import com.ssd.mvd.interfaces.EntityToCassandraConverter;
import com.ssd.mvd.interfaces.DatabaseCommonMethods;
import com.ssd.mvd.interfaces.ServiceCommonMethods;
import com.ssd.mvd.constants.CassandraFunctions;
import com.ssd.mvd.inspectors.EntitiesInstances;
import com.ssd.mvd.constants.CassandraCommands;
import com.ssd.mvd.entity.patrulDataSet.Patrul;
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
import java.util.Map;

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
        this.getSession().execute(
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
                    EntitiesInstances.TUPLE_OF_CAR.generate(
                            this.getRowFromTabletsKeyspace(
                                    EntitiesInstances.TUPLE_OF_CAR,
                                    "uuid",
                                    tupleOfCar.getUuid().toString()
                            )
                    )
            ).flatMap( tupleOfCar1 -> {
                    final Optional< TupleOfCar > optional = Optional.of( tupleOfCar );

                    if (
                            optional.filter( tupleOfCar2 -> !tupleOfCar1.getTrackerId().equals( tupleOfCar.getTrackerId() )
                                    && !super.check( tupleOfCar.getTrackerId() ) ).isPresent()
                    ) {
                        return super.getResponse( Map.of( "message", "Wrong TrackerId", "code", 201 ) );
                    }

                    final StringBuilder stringBuilder = super.newStringBuilder();

                    if (
                            optional.filter( tupleOfCar2 -> super.objectIsNotNull( tupleOfCar1.getUuidOfPatrul() )
                                    && super.objectIsNotNull( tupleOfCar.getUuidOfPatrul() )
                                    && tupleOfCar1.getUuidOfPatrul().compareTo( tupleOfCar.getUuidOfPatrul() ) != 0 ).isPresent()
                    ) {
                        stringBuilder.append(
                                MessageFormat.format(
                                        """
                                        {0} {1}.{2}
                                        SET uuidforescortcar = {3}
                                        WHERE uuid = {4};
                                        """,
                                        CassandraCommands.UPDATE,

                                        CassandraTables.TABLETS,
                                        CassandraTables.PATRULS,

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

                                        CassandraTables.TABLETS,
                                        CassandraTables.PATRULS,
                                        null,
                                        tupleOfCar1.getUuidOfPatrul()
                                )
                        );
                    }

                    stringBuilder.append(
                            tupleOfCar.getEntityInsertCommand()
                    ).append( CassandraCommands.APPLY_BATCH );

                    return this.getSession().execute( stringBuilder.toString() ).wasApplied()
                            ? super.getResponse(
                                    Map.of( "message", "Car" + tupleOfCar.getGosNumber() + " was updated successfully" )
                            )
                            : super.getResponse(
                                    Map.of(
                                            "message", "This car does not exists",
                                            "code", 201,
                                            "success", false
                                    )
                            );
                } );

    public final Function< String, Mono< ApiResponseModel > > deleteTupleOfCar = uuid ->
            super.convert(
                    EntitiesInstances.TUPLE_OF_CAR.generate(
                            this.getRowFromTabletsKeyspace(
                                    EntitiesInstances.TUPLE_OF_CAR,
                                    "uuid",
                                    uuid
                            )
                    )
            ).flatMap( tupleOfCar1 -> !super.objectIsNotNull( tupleOfCar1.getUuidOfPatrul() )
                    && !super.objectIsNotNull( tupleOfCar1.getUuidOfEscort() )
                    ? super.getResponse(
                            Map.of(
                                    "message", uuid + " was removed successfully",
                                    "success", tupleOfCar1.delete()
                            )
                    )
                    : super.getResponse(
                            Map.of(
                                    "message", "You cannot delete this car, it is linked to Patrul or Escort",
                                    "code", 201,
                                    "success", false
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
                                    EntitiesInstances.PATRUL.generate(
                                            this.getRowFromTabletsKeyspace(
                                                    EntitiesInstances.PATRUL,
                                                    tupleOfCar.getUuidOfPatrul().toString(),
                                                    "uuid"
                                            )
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
                                        .writeToKafkaTupleOfCar
                                        .accept( tupleOfCar );

                                /*
                                сохраняем в статичный кэш
                                 */
                                tupleOfCarMap.putIfAbsent(
                                        tupleOfCar.getTrackerId(),
                                        this.saveTackerInfo.apply( new TrackerInfo( patrul, tupleOfCar ) )
                                );

                                return super.getResponse(
                                        Map.of(
                                                "message", "Escort was saved successfully",
                                                "success", true
                                        )
                                );
                            } )
                            : super.getResponse(
                                    Map.of(
                                            "message", "Escort was saved successfully",
                                            "success", tupleOfCarMap.putIfAbsent(
                                                    tupleOfCar.getTrackerId(),
                                                    this.saveTackerInfo.apply( new TrackerInfo( tupleOfCar ) )
                                            )
                                    )
                            )
                    : super.getResponse( Map.of( "message", "This car is already exists", "code", 201 ) )
            : super.getResponse(
                    Map.of(
                            "message", "This trackers or gosnumber is already registered to another car, so choose another one",
                            "code", 201
                    )
            );

    public final Function< String, Mono< TrackerInfo > > getCurrentTracker = trackerId -> super.convert(
                    this.getRowFromTabletsKeyspace(
                            EntitiesInstances.TRACKER_INFO,
                            "trackersId",
                            super.joinWithAstrix( trackerId )
                    )
            ).map( row -> {
                    final TupleOfCar tupleOfCar = EntitiesInstances.TUPLE_OF_CAR.generate(
                            this.getRowFromTabletsKeyspace(
                                    EntitiesInstances.TUPLE_OF_CAR,
                                    "gosNumber",
                                    row.getString( "gosnumber" )
                            )
                    );

                    if ( super.objectIsNotNull( tupleOfCar.getUuidOfPatrul() ) ) {
                        final Patrul patrul = EntitiesInstances.PATRUL.generate(
                                this.getRowFromTabletsKeyspace(
                                        EntitiesInstances.PATRUL,
                                        "uuid",
                                        tupleOfCar.getUuidOfPatrul().toString()
                                )
                        );

                        return new TrackerInfo( patrul, tupleOfCar, row );
                    }

                    return new TrackerInfo( tupleOfCar, row );
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
                final TupleOfCar tupleOfCar = EntitiesInstances.TUPLE_OF_CAR.generate(
                        this.getRowFromTabletsKeyspace(
                                EntitiesInstances.TUPLE_OF_CAR,
                                "gosNumber",
                                super.joinWithAstrix( row.getString( "gosnumber" ) )
                        )
                );

                if ( super.objectIsNotNull( tupleOfCar.getUuidOfPatrul() ) ) {
                    return new TrackerInfo(
                            EntitiesInstances.PATRUL.generate(
                                    this.getRowFromTabletsKeyspace(
                                            EntitiesInstances.PATRUL,
                                            "uuid",
                                            tupleOfCar.getUuidOfPatrul().toString()
                                    )
                            ),
                            tupleOfCar,
                            row
                    );
                }

                return new TrackerInfo( tupleOfCar, row );
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
        cassandraDataControl = null;
        super.logging( this );
        this.getSession().close();
    }
}
