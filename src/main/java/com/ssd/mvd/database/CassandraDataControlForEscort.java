package com.ssd.mvd.database;

import com.ssd.mvd.constants.CassandraFunctions;
import com.ssd.mvd.constants.CassandraCommands;
import com.ssd.mvd.entity.patrulDataSet.Patrul;
import com.ssd.mvd.constants.CassandraTables;
import com.ssd.mvd.kafka.KafkaDataControl;
import com.ssd.mvd.entity.*;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Row;

import reactor.core.scheduler.Schedulers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.text.MessageFormat;
import java.util.function.*;
import java.util.Optional;
import java.util.UUID;
import java.util.List;
import java.util.Map;

@lombok.Data
public final class CassandraDataControlForEscort extends CassandraConverter {
    private final Session session = CassandraDataControl.getInstance().getSession();
    private final Cluster cluster = CassandraDataControl.getInstance().getCluster();

    private static CassandraDataControlForEscort cassandraDataControl = new CassandraDataControlForEscort();

    public static CassandraDataControlForEscort getInstance () {
        return cassandraDataControl != null
                ? cassandraDataControl
                : ( cassandraDataControl = new CassandraDataControlForEscort() );
    }

    private CassandraDataControlForEscort () {
        super.logging( "CassandraDataControlForEscort is ready" );
    }

    public Row getRowFromEscortKeyspace(
            final CassandraTables cassandraTableName,
            final String columnName,
            final Object paramName
    ) {
        return this.getSession().execute(
                MessageFormat.format(
                        """
                        {0} {1}.{2} WHERE {3} = {4};
                        """,
                        CassandraCommands.SELECT_ALL,
                        CassandraTables.ESCORT,
                        cassandraTableName,
                        columnName,
                        paramName
                )
        ).one();
    }

    /*
        после получения сигнала от трекера обновляем его значения в БД
    */
    private final Function< TrackerInfo, TrackerInfo > saveTackerInfo = trackerInfo -> {
        this.getSession().execute(
                MessageFormat.format(
                        """
                        {0} {1}.{2}
                        ( trackersId, patrulPassportSeries, gosnumber, status
                        latitude, longitude, totalActivityTime, lastActiveDate, dateOfRegistration )
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

    private final Function< TupleOfCar, Mono< ApiResponseModel > > updateEscortCar = tupleOfCar ->
            this.getGetCurrentTupleOfCar().apply( tupleOfCar.getUuid() )
                .flatMap( tupleOfCar1 -> {
                    final Optional< TupleOfCar > optional = Optional.of( tupleOfCar );
                    if ( optional.filter( tupleOfCar2 -> !tupleOfCar1.getTrackerId().equals( tupleOfCar.getTrackerId() )
                            && !super.check( tupleOfCar.getTrackerId() ) )
                            .isPresent() ) {
                        return super.getResponse( Map.of( "message", "Wrong TrackerId", "code", 201 ) );
                    }

                    final StringBuilder stringBuilder = super.newStringBuilder();

                    if ( optional.filter( tupleOfCar2 -> super.objectIsNotNull( tupleOfCar1.getUuidOfPatrul() )
                            && super.objectIsNotNull( tupleOfCar.getUuidOfPatrul() )
                            && tupleOfCar1.getUuidOfPatrul().compareTo( tupleOfCar.getUuidOfPatrul() ) != 0 )
                            .isPresent() ) {
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
                            MessageFormat.format(
                                    """
                                    {0} {1}.{2} {3}
                                    VALUES( {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14} );
                                    """,
                                    CassandraCommands.INSERT_INTO,
                                    CassandraTables.ESCORT,
                                    CassandraTables.TUPLE_OF_CAR,
                                    super.getALlParamsNamesForClass.apply( TupleOfCar.class ),

                                    tupleOfCar.getUuid(),
                                    tupleOfCar.getUuidOfEscort(),
                                    tupleOfCar.getUuidOfPatrul(),

                                    super.joinWithAstrix( tupleOfCar.getCarModel() ),
                                    super.joinWithAstrix( tupleOfCar.getGosNumber() ),
                                    super.joinWithAstrix( tupleOfCar.getTrackerId() ),
                                    super.joinWithAstrix( tupleOfCar.getNsfOfPatrul() ),
                                    super.joinWithAstrix( tupleOfCar.getSimCardNumber() ),

                                    tupleOfCar.getLatitude(),
                                    tupleOfCar.getLongitude(),
                                    tupleOfCar.getAverageFuelConsumption()
                            )
                    ).append( CassandraCommands.APPLY_BATCH );

                    return this.getSession().execute( stringBuilder.toString() ).wasApplied()
                            ? super.getResponse(
                                    Map.of( "message", "Car" + tupleOfCar.getGosNumber() + " was updated successfully" ) )
                            : super.getResponse(
                                    Map.of( "message", "This car does not exists",
                                            "code", 201,
                                            "success", false ) );
                } );

    public void updateEscortCarLocation (
            final Double longitude,
            final Double latitude,
            final TupleOfCar tupleOfCar ) {
        this.getSession().execute (
                MessageFormat.format(
                        """
                        {0} {1}.{2}
                        SET longitude = {3}, latitude = {4}
                        WHERE uuid = {5} AND trackerid = {6};
                        """,
                        CassandraCommands.UPDATE,
                        CassandraTables.ESCORT,
                        CassandraTables.TUPLE_OF_CAR,
                        longitude,
                        latitude,
                        tupleOfCar.getUuid(),
                        super.joinWithAstrix( tupleOfCar.getTrackerId() )
                )
        );
    }

    private final Function< UUID, Mono< TupleOfCar > > getCurrentTupleOfCar = uuid ->
            super.convert( new TupleOfCar(
                    this.getRowFromEscortKeyspace(
                            CassandraTables.TUPLE_OF_CAR,
                            "uuid",
                            uuid
                    )
            ) );

    private final Function< String, Mono< ApiResponseModel > > deleteTupleOfCar = uuid ->
            this.getGetCurrentTupleOfCar().apply( UUID.fromString( uuid ) )
                    .flatMap( tupleOfCar1 -> !super.objectIsNotNull( tupleOfCar1.getUuidOfPatrul() )
                            && !super.objectIsNotNull( tupleOfCar1.getUuidOfEscort() )
                            ? super.getResponse(
                                    Map.of( "message", uuid + " was removed successfully",
                                            "success", this.getSession().execute(
                                                    MessageFormat.format(
                                                            """
                                                            {0} {1} {2} {3}
                                                            """,
                                                            CassandraCommands.BEGIN_BATCH,
                                                            MessageFormat.format(
                                                                    """
                                                                    {0} {1}.{2} WHERE uuid = {3};
                                                                    """,
                                                                    CassandraCommands.DELETE,
                                                                    CassandraTables.ESCORT,
                                                                    CassandraTables.TUPLE_OF_CAR,
                                                                    UUID.fromString( uuid )
                                                            ),
                                                            MessageFormat.format(
                                                                    """
                                                                    {0} {1}.{2} WHERE trackersId = {3} {4};
                                                                    """,
                                                                    CassandraCommands.DELETE,
                                                                    CassandraTables.ESCORT,
                                                                    CassandraTables.TRACKERSID,
                                                                    tupleOfCar1.getTrackerId(),
                                                                    CassandraCommands.IF_EXISTS
                                                            ),
                                                            CassandraCommands.APPLY_BATCH ) )
                                                    .wasApplied() ) )
                            : super.getResponse(
                                    Map.of( "message", "You cannot delete this car, it is linked to Patrul or Escort",
                                            "code", 201,
                                            "success", false ) ) );

    private final Function< TupleOfCar, Mono< ApiResponseModel > > saveNewTupleOfCar = tupleOfCar ->
            super.check( tupleOfCar.getTrackerId() )
            && super.checkCarNumber( tupleOfCar.getGosNumber() )
                    ? this.getSession().execute(
                            MessageFormat.format(
                                    """
                                    {0} {1}.{2} {3}
                                    VALUES ( {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14} );
                                    """,
                                    CassandraCommands.INSERT_INTO,
                                    CassandraTables.ESCORT,
                                    CassandraTables.TUPLE_OF_CAR,
                                    super.getALlParamsNamesForClass.apply( TupleOfCar.class ),

                                    CassandraFunctions.UUID,
                                    tupleOfCar.getUuidOfEscort(),
                                    tupleOfCar.getUuidOfPatrul(),

                                    super.joinWithAstrix( tupleOfCar.getCarModel() ),
                                    super.joinWithAstrix( tupleOfCar.getGosNumber() ),
                                    super.joinWithAstrix( tupleOfCar.getTrackerId() ),
                                    super.joinWithAstrix( tupleOfCar.getNsfOfPatrul() ),
                                    super.joinWithAstrix( tupleOfCar.getSimCardNumber() ),

                                    tupleOfCar.getLatitude(),
                                    tupleOfCar.getLongitude(),
                                    tupleOfCar.getAverageFuelConsumption() ) )
                    .wasApplied()
                    /*
                    проверяем что Эскорт сявзан с каким-либо патрульным
                     */
                    ? super.objectIsNotNull( tupleOfCar.getUuidOfPatrul() )
                            /*
                            если да, то находим патрульного и связываем его с эскортом
                             */
                            ? super.convert(
                                CassandraDataControl
                                        .getInstance()
                                        .getGetPatrul()
                                        .apply( tupleOfCar.getUuidOfPatrul().toString(), 1 ) )
                            .flatMap( patrul -> {
                                /*
                                соединяем патрульного с ID эскорт машины
                                 */
                                patrul.linkWithTupleOfCar( tupleOfCar );

                                this.getSession().execute(
                                        MessageFormat.format(
                                                """
                                                {0} {1}.{2}
                                                SET uuidForEscortCar = {3},
                                                carType = {4},
                                                carNumber = {5},
                                                WHERE uuid = {6};
                                                """,
                                                CassandraCommands.UPDATE,
                                                CassandraTables.TABLETS,
                                                CassandraTables.PATRULS,
                                                patrul.getPatrulUniqueValues().getUuidForEscortCar(),
                                                super.joinWithAstrix( patrul.getPatrulCarInfo().getCarType() ),
                                                super.joinWithAstrix( patrul.getPatrulCarInfo().getCarNumber() ),
                                                patrul.getUuid()
                                        )
                                );

                                /*
                                отправляем уведомлнеие через Кафку,
                                о том что новый эскорт был добален в БД
                                 */
                                KafkaDataControl
                                        .getInstance()
                                        .getWriteToKafkaTupleOfCar()
                                        .accept( tupleOfCar );

                                /*
                                сохраняем в статичный кэш
                                 */
                                super.tupleOfCarMap.putIfAbsent(
                                        tupleOfCar.getTrackerId(),
                                        this.getSaveTackerInfo().apply( new TrackerInfo( patrul, tupleOfCar ) )
                                );

                                return super.getResponse(
                                        Map.of( "message", "Escort was saved successfully",
                                                "success", true ) );
                            } )
                            : super.getResponse(
                                    Map.of( "message", "Escort was saved successfully",
                                            "success", super.tupleOfCarMap.putIfAbsent(
                                                    tupleOfCar.getTrackerId(),
                                                    this.getSaveTackerInfo().apply( new TrackerInfo( tupleOfCar ) )
                                            ) ) )
                    : super.getResponse( Map.of( "message", "This car is already exists", "code", 201 ) )
            : super.getResponse(
                    Map.of( "message", "This trackers or gosnumber is already registered to another car, so choose another one",
                            "code", 201 ) );

    private final Function< String, Mono< TupleOfCar > > getTupleOfCarByTracker = trackersId -> super.convert(
            new TupleOfCar(
                    this.getRowFromEscortKeyspace(
                            CassandraTables.TUPLE_OF_CAR,
                            "trackerId",
                            super.joinWithAstrix( trackersId ) ) ) );

    private final Function< String, Mono< TrackerInfo > > getCurrentTracker = trackerId -> super.convert(
            this.getRowFromEscortKeyspace(
                    CassandraTables.TRACKERSID,
                    "trackersId",
                    super.joinWithAstrix( trackerId ) ) )
            .map( row -> {
                final TupleOfCar tupleOfCar = new TupleOfCar(
                        this.getRowFromEscortKeyspace(
                                CassandraTables.TUPLE_OF_CAR,
                                "gosNumber",
                                row.getString( "gosnumber" )
                        )
                );

                if ( super.objectIsNotNull( tupleOfCar.getUuidOfPatrul() ) ) {
                    final Patrul patrul = new Patrul(
                            CassandraDataControl
                                    .getInstance()
                                    .getRowFromTabletsKeyspace(
                                            CassandraTables.PATRULS,
                                            "uuid",
                                            tupleOfCar.getUuidOfPatrul()
                                    )
                    );

                    return new TrackerInfo( patrul, tupleOfCar, row );
                }

                return new TrackerInfo( tupleOfCar, row );
            } );

    private final BiFunction< String, String, Mono< TupleOfCar > > getTupleOfCar = ( gosNumber, trackersId ) -> {
            try {
                return super.convert( new TupleOfCar(
                        this.getRowFromEscortKeyspace(
                                CassandraTables.TUPLE_OF_CAR,
                                "gosNumber",
                                super.joinWithAstrix( gosNumber )
                        )
                ) );
            } catch ( final Exception e ) {
                super.logging( e.getMessage() );

                this.getSession().execute(
                        MessageFormat.format(
                                """
                                {0} {1} {2} WHERE trackersId = {3};
                                """,
                                CassandraCommands.DELETE,
                                CassandraTables.ESCORT,
                                CassandraTables.TRACKERSID,
                                super.joinWithAstrix( trackersId )
                        )
                );

                return Mono.empty();
            }
    };

    private final Supplier< Flux< TrackerInfo > > getAllTrackers = () -> CassandraDataControl
            .getInstance()
            .getGetAllEntities()
            .apply( CassandraTables.ESCORT, CassandraTables.TRACKERSID )
            .map( row -> {
                final TupleOfCar tupleOfCar = new TupleOfCar(
                        this.getRowFromEscortKeyspace(
                                CassandraTables.TUPLE_OF_CAR,
                                "gosNumber",
                                super.joinWithAstrix( row.getString( "gosnumber" ) )
                        )
                );

                if ( super.objectIsNotNull( tupleOfCar.getUuidOfPatrul() ) ) {
                    final Patrul patrul = new Patrul(
                            CassandraDataControl
                                    .getInstance()
                                    .getRowFromTabletsKeyspace(
                                            CassandraTables.PATRULS,
                                            "uuid",
                                            tupleOfCar.getUuidOfPatrul()
                                    )
                    );

                    return new TrackerInfo( patrul, tupleOfCar, row );
                }

                return new TrackerInfo( tupleOfCar, row );
            } )
            .sequential()
            .publishOn( Schedulers.single() );

    private final Function< Point, Flux< TupleOfCar > > findTheClosestCarsInRadius = point -> CassandraDataControl
            .getInstance()
            .getGetAllEntities()
            .apply( CassandraTables.ESCORT, CassandraTables.TUPLE_OF_CAR )
            .filter( row -> super.calculate( point, row ) <= point.getRadius() )
            .map( TupleOfCar::new )
            .sequential()
            .publishOn( Schedulers.single() );

    private final Function< List< Point >, Flux< TupleOfCar > > findTheClosestCarsInPolygon = point ->
            CassandraDataControl
                .getInstance()
                .getGetAllEntities()
                .apply( CassandraTables.ESCORT, CassandraTables.TUPLE_OF_CAR )
                .filter( row -> super.calculateDistanceInSquare( point, row ) )
                .map( TupleOfCar::new )
                .sequential()
                .publishOn( Schedulers.single() );
}
