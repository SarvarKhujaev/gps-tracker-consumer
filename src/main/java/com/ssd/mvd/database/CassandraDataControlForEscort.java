package com.ssd.mvd.database;

import com.ssd.mvd.interfaces.DatabaseCommonMethods;
import com.ssd.mvd.interfaces.ServiceCommonMethods;
import com.ssd.mvd.constants.CassandraFunctions;
import com.ssd.mvd.constants.CassandraCommands;
import com.ssd.mvd.entity.patrulDataSet.Patrul;
import com.ssd.mvd.constants.CassandraTables;
import com.ssd.mvd.kafka.KafkaDataControl;
import com.ssd.mvd.entity.*;

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

public final class CassandraDataControlForEscort
        extends CassandraConverter
        implements DatabaseCommonMethods, ServiceCommonMethods {
    private final Session session = CassandraDataControl.getInstance().getSession();

    private static CassandraDataControlForEscort cassandraDataControl = new CassandraDataControlForEscort();

    public static CassandraDataControlForEscort getInstance () {
        return cassandraDataControl != null
                ? cassandraDataControl
                : ( cassandraDataControl = new CassandraDataControlForEscort() );
    }

    @Override
    public Session getSession() {
        return this.session;
    }

    private CassandraDataControlForEscort () {
        super.logging( this );
    }

    /*
    возвращает ROW из БД для любой таблицы внутри TABLETS
    */
    @Override
    public Row getRowFromTabletsKeyspace (
            // название таблицы внутри Tablets
            final CassandraTables cassandraTableName,
            // название колонки
            final String columnName,
            // параметр по которому введется поиск
            final String paramName
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

    @Override
    public <T> List< Row > getListOfEntities (
            // название таблицы внутри Tablets
            final CassandraTables cassandraTableName,
            // название колонки
            final String columnName,
            // параметр по которому введется поиск
            final List< T > ids
    ) {
        return this.getSession().execute(
                MessageFormat.format(
                        """
                        {0} {1}.{2} WHERE {3} IN {4};
                        """,
                        CassandraCommands.SELECT_ALL,

                        CassandraTables.ESCORT,

                        cassandraTableName,
                        columnName,
                        super.newStringBuilder( "(" ).append( super.convertListToCassandra.apply( ids ) ).append( ")" )
                )
        ).all();
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

    public final Function< TupleOfCar, Mono< ApiResponseModel > > updateEscortCar = tupleOfCar ->
            super.convert(
                    new TupleOfCar(
                            CassandraDataControlForEscort
                                    .getInstance()
                                    .getRowFromTabletsKeyspace(
                                            CassandraTables.TUPLE_OF_CAR,
                                            "uuid",
                                            tupleOfCar.getUuid().toString()
                                    )
                    )
            ).flatMap( tupleOfCar1 -> {
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

    public final Function< UUID, Mono< TupleOfCar > > getCurrentTupleOfCar = uuid ->
            super.convert( new TupleOfCar(
                    this.getRowFromTabletsKeyspace(
                            CassandraTables.TUPLE_OF_CAR,
                            "uuid",
                            uuid.toString()
                    )
            ) );

    public final Function< String, Mono< ApiResponseModel > > deleteTupleOfCar = uuid ->
            super.convert(
                    new TupleOfCar(
                            CassandraDataControlForEscort
                                    .getInstance()
                                    .getRowFromTabletsKeyspace(
                                            CassandraTables.TUPLE_OF_CAR,
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
                                    new Patrul(
                                            this.getRowFromTabletsKeyspace(
                                                    CassandraTables.PATRULS,
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
                        CassandraTables.TRACKERSID,
                        "trackersId",
                        super.joinWithAstrix( trackerId )
                )
            ).map( row -> {
                final TupleOfCar tupleOfCar = new TupleOfCar(
                        this.getRowFromTabletsKeyspace(
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
            .apply( CassandraTables.ESCORT, CassandraTables.TRACKERSID )
            .map( row -> {
                final TupleOfCar tupleOfCar = new TupleOfCar(
                        this.getRowFromTabletsKeyspace(
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
                                            tupleOfCar.getUuidOfPatrul().toString()
                                    )
                    );

                    return new TrackerInfo( patrul, tupleOfCar, row );
                }

                return new TrackerInfo( tupleOfCar, row );
            } )
            .sequential()
            .publishOn( Schedulers.single() );

    public final Function< Point, Flux< TupleOfCar > > findTheClosestCarsInRadius = point ->
            CassandraDataControl
                .getInstance()
                .getAllEntities
                .apply( CassandraTables.ESCORT, CassandraTables.TUPLE_OF_CAR )
                .filter( row -> super.calculate( point, row ) <= point.getRadius() )
                .map( TupleOfCar::new )
                .sequential()
                .publishOn( Schedulers.single() );

    public final Function< List< Point >, Flux< TupleOfCar > > findTheClosestCarsInPolygon = point ->
            CassandraDataControl
                .getInstance()
                .getAllEntities
                .apply( CassandraTables.ESCORT, CassandraTables.TUPLE_OF_CAR )
                .filter( row -> super.calculateDistanceInSquare( point, row ) )
                .map( TupleOfCar::new )
                .sequential()
                .publishOn( Schedulers.single() );

    @Override
    public void close() {
        cassandraDataControl = null;
        super.logging( this );
        this.getSession().close();
    }
}
