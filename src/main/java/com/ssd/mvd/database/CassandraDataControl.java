package com.ssd.mvd.database;

import java.util.Date;
import java.util.Calendar;
import java.time.Duration;
import java.text.MessageFormat;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.core.publisher.ParallelFlux;

import com.datastax.driver.core.*;

import com.ssd.mvd.entity.*;
import com.ssd.mvd.kafka.KafkaDataControl;
import com.ssd.mvd.constants.CassandraTables;
import com.ssd.mvd.constants.CassandraCommands;
import com.ssd.mvd.entity.patrulDataSet.Patrul;
import com.ssd.mvd.constants.CassandraFunctions;
import com.ssd.mvd.subscribers.CustomSubscriber;
import com.ssd.mvd.interfaces.ServiceCommonMethods;
import com.ssd.mvd.interfaces.DatabaseCommonMethods;
import com.ssd.mvd.entity.patrulDataSet.PatrulFuelStatistics;
import com.ssd.mvd.database.cassandraConfigs.CassandraParamsAndOptionsStore;

@lombok.Data
public final class CassandraDataControl
        extends CassandraParamsAndOptionsStore
        implements DatabaseCommonMethods, ServiceCommonMethods {
    private final Cluster cluster;
    private final Session session;

    private static CassandraDataControl instance = new CassandraDataControl();

    /*
    создаем, регистрируем и сохраняем все таблицы, типы и кодеки
    */
    public void setCassandraTablesAndTypesRegister () {
        CassandraTablesAndTypesRegister.generate( this.getSession() );
        this.register();
    }

    private void register () {
        this.getAllEntities.apply( CassandraTables.TABLETS, CassandraTables.POLICE_TYPE )
                .sequential()
                .publishOn( Schedulers.single() )
                .subscribe( row -> super.icons.put( row.getString( "policeType" ), new Icons( row ) ) );

        this.getGetAllTrackers().apply( false )
                .subscribe( trackerInfo -> super.trackerInfoMap.putIfAbsent( trackerInfo.getTrackerId(), trackerInfo ) );

        CassandraDataControlForEscort
                .getInstance()
                .getGetAllTrackers()
                .get()
                .subscribe( trackerInfo -> super.tupleOfCarMap.putIfAbsent( trackerInfo.getTrackerId(), trackerInfo ) );
    }

    public static CassandraDataControl getInstance () {
        return instance != null ? instance : ( instance = new CassandraDataControl() );
    }

    private CassandraDataControl () {
        this.session = (
                this.cluster = Cluster
                        .builder()
                        .withPort( super.getPort() )
                        .addContactPoint( CassandraParamsAndOptionsStore.HOST )
                        .withClusterName( CassandraParamsAndOptionsStore.CLUSTER_NAME )
                        .withProtocolVersion( ProtocolVersion.V4 )
                        .withRetryPolicy( super.getCustomRetryPolicy() )
                        .withQueryOptions(
                                new QueryOptions()
                                        .setDefaultIdempotence( true )
                                        .setConsistencyLevel( ConsistencyLevel.ONE )
                        ).withSocketOptions( super.getSocketOptions() )
                        .withPoolingOptions( super.getPoolingOptions() )
                        .withCompression( ProtocolOptions.Compression.LZ4 )
                        .build()
        ).connect();

        super.logging( this.getClass() );

        /*
        создаем, регистрируем и сохраняем все таблицы, типы и кодеки
        */
        CassandraTablesAndTypesRegister.generate(
                this.getSession()
        );
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

                        CassandraTables.TRACKERS,

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

                        CassandraTables.TRACKERS,

                        cassandraTableName,
                        columnName,
                        super.newStringBuilder( "(" ).append( super.convertListToCassandra.apply( ids ) ).append( ")" )
                )
        ).all();
    }

    private final Consumer< ReqCar > updateReqCarPosition = reqCar ->
            this.getSession().executeAsync(
                    MessageFormat.format(
                            """
                            {0} {1}.{2}
                            SET longitude = {3}, latitude = {4}
                            WHERE uuid = {5};
                            """,
                            CassandraCommands.UPDATE,

                            CassandraTables.TABLETS,
                            CassandraTables.CARS,

                            reqCar.getLongitude(),
                            reqCar.getLatitude(),
                            reqCar.getUuid()
                    )
            );

    private final BiConsumer< Position, TrackerInfo > updateTrackerInfoAndCarLocation = ( updatedPosition, trackerInfo ) ->
            this.getSession().execute(
                    MessageFormat.format(
                            """
                            {0} {1} {2} {3}
                            """,
                            /*
                            запускаем BATCH
                             */
                            CassandraCommands.BEGIN_BATCH,

                            /*
                            сохраняем локацию машин эскорта
                            сохраняются данные всех трекеров
                            */
                            MessageFormat.format(
                                    """
                                    {0} {1}.{2}
                                    ( imei, date, speed, altitude, longitude, address )
                                    VALUES ( {3}, {4}, {5}, {6}, {7}, '' );
                                    """,
                                    CassandraCommands.INSERT_INTO,

                                    CassandraTables.ESCORT,
                                    CassandraTables.ESCORT_LOCATION,

                                    super.joinWithAstrix( updatedPosition.getDeviceId() ),
                                    super.joinWithAstrix( updatedPosition.getDeviceTime() ),

                                    updatedPosition.getSpeed(),
                                    updatedPosition.getLongitude(),
                                    updatedPosition.getLatitude()
                            ),

                            /*
                                после получения сигнала от трекера обновляем его значения в БД
                            */
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
                            ),
                            /*
                            завершаем BATCH
                             */
                            CassandraCommands.APPLY_BATCH
                    )
            );

    private final Function< Position, String > saveCarLocation = position -> {
            final Optional< Position > optional = Optional.of( position );
            optional.filter( position1 -> !super.check( position.getDeviceId() ) )
                    .ifPresent( position1 -> super.unregisteredTrackers.put(
                            position.getDeviceId(),
                            super.newDate() ) );

            /*
            для начала проверяем какому отделу принадлежит машина
            Эскорт или Патрулю
            */
            if ( optional.filter( position1 -> super.check( position.getDeviceId() ) ).isPresent() ) {
                CassandraDataControlForEscort
                        .getInstance()
                        // находим машину по IMEI трекера
                        .getGetTupleOfCarByTracker()
                        .apply( position.getDeviceId() )
                        // проверяем что такая машина существует
                        .filter( tupleOfCar -> super.objectIsNotNull( tupleOfCar.getUuid() )
                                && super.objectIsNotNull( tupleOfCar.getGosNumber() ) )
                        .subscribe( new CustomSubscriber<>(
                                tupleOfCar -> {
                                    /*
                                    проверяем не прикреплена ли машина Эскорта к патрульному
                                    */
                                    if ( super.objectIsNotNull( tupleOfCar.getUuidOfPatrul() ) ) {
                                        final Patrul patrul = this.getGetPatrul()
                                                .apply( tupleOfCar.getUuidOfPatrul().toString(), 1 );

                                        /*
                                        обновляем данные самого трекера,
                                        данными машины и патрульного
                                         */
                                        final Position updatedPosition = super.tupleOfCarMap
                                                .get( position.getDeviceId() )
                                                .updateTime( position, tupleOfCar, patrul );

                                        /*
                                        сохраняем все обновленные данные в БД
                                         */
                                        this.updateTrackerInfoAndCarLocation.accept(
                                                updatedPosition,
                                                super.tupleOfCarMap.get( position.getDeviceId() )
                                        );

                                        /*
                                            отправляем обновленные данные о позиции машины в Кафку
                                         */
                                        KafkaDataControl
                                                .getInstance()
                                                .getWriteToKafkaEscort()
                                                .accept( updatedPosition );
                                    } else {
                                        /*
                                        обновляем данные самого трекера,
                                        данными машины и патрульного
                                        */
                                        final Position updatedPosition = super.tupleOfCarMap
                                                .get( position.getDeviceId() )
                                                .updateTime( position, tupleOfCar );

                                        /*
                                        сохраняем все обновленные данные в БД
                                        */
                                        this.updateTrackerInfoAndCarLocation.accept(
                                                updatedPosition,
                                                super.tupleOfCarMap.get( position.getDeviceId() )
                                        );

                                        /*
                                        отправляем обновленные данные о позиции машины в Кафку
                                        */
                                        KafkaDataControl
                                                .getInstance()
                                                .getWriteToKafkaEscort()
                                                .accept( updatedPosition );
                                    }
                                }
                        ) );
            }

            else {
                super.convert( this.getGetCarByNumber().apply( "trackerId", position.getDeviceId() ) )
                        .filter( super::check )
                        .subscribe( new CustomSubscriber<>(
                                reqCar -> {
                                    // убираем уже зарегистрированный трекер
                                    super.unregisteredTrackers.remove( optional.get().getDeviceId() );

                                    if ( super.check( optional.get().getDeviceId() ) ) {
                                        // сохраняем в базу только если машина двигается
                                        if ( super.check( optional.get() ) ) {
                                            this.getSession().executeAsync(
                                                    MessageFormat.format(
                                                            """
                                                            {0} {1}.{2}
                                                            ( imei, date, speed, latitude, longitude, address )
                                                            VALUES ( {3}, {4}, {5}, {6}, {7}, '' )
                                                            """,
                                                            CassandraCommands.INSERT_INTO,
                                                            CassandraTables.TRACKERS,
                                                            CassandraTables.TRACKERS_LOCATION_TABLE,

                                                            super.joinWithAstrix( optional.get().getDeviceId() ),
                                                            super.joinWithAstrix( optional.get().getDeviceTime() ),

                                                            optional.get().getSpeed(),
                                                            optional.get().getLongitude(),
                                                            optional.get().getLatitude()
                                                    )
                                            );
                                        }

                                        final Patrul patrul = this.getGetPatrul().apply( reqCar.getPatrulPassportSeries(), 0 );

                                        KafkaDataControl
                                                .getInstance()
                                                .getWriteToKafkaPosition()
                                                .accept( super.trackerInfoMap
                                                        .get( optional.get().getDeviceId() )
                                                        .updateTime( optional.get(), reqCar, patrul ) );
                                    }

                                    // срабатывает когда приходит сигнал от нового трекера на новой машины
                                    else {
                                        final Patrul patrul = this.getGetPatrul().apply( reqCar.getPatrulPassportSeries(), 0 );

                                        final TrackerInfo trackerInfo = new TrackerInfo( patrul, reqCar );

                                        KafkaDataControl
                                                .getInstance()
                                                .getWriteToKafka()
                                                .accept( reqCar );

                                        super.trackerInfoMap.put(
                                                reqCar.getTrackerId(),
                                                this.getAddTackerInfo().apply( trackerInfo ) );
                                    }
                                }
                        ) );
            }

            return position.getDeviceId();
    };

    /*
    сохраняет новый трекер
    */
    private final Function< TrackerInfo, TrackerInfo > addTackerInfo = trackerInfo -> {
            this.getSession().execute(
                    MessageFormat.format(
                            """
                            {0} {1}.{2}
                            ( trackersId,
                            patrulPassportSeries,
                            gosnumber,
                            policeType,
                            policeType2,
                            status,
                            latitude,
                            longitude,
                            totalActivityTime,
                            lastActiveDate,
                            dateOfRegistration )
                            VALUES( {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11,number,#}, {12}, {13} );
                            """,
                            CassandraCommands.INSERT_INTO,
                            CassandraTables.TRACKERS,
                            CassandraTables.TRACKERSID,

                            super.joinWithAstrix( trackerInfo.getTrackerId() ),
                            super.joinWithAstrix( trackerInfo.getPatrulPassportSeries() ),
                            super.joinWithAstrix( trackerInfo.getGosNumber() ),
                            super.joinWithAstrix( trackerInfo.getIcon() ),
                            super.joinWithAstrix( trackerInfo.getIcon2() ),
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

    // сохраняем скорость машины и вычислем какое расстояние она проехала после предыдущего сигнала
    private final BiConsumer< TrackerInfo, Double > saveFuelConsumptionOfCar = ( trackerInfo, speed ) ->
            Optional.of( speed ).filter( speed1 -> speed1 > 0 )
                    .ifPresent( speed1 -> this.getSession().executeAsync(
                            MessageFormat.format(
                                    """
                                    {0} {1}.{2}
                                    ( imei, date, speed, distance )
                                    VALUES( {3}, {4}, {5}, {6} );
                                    """,
                                    CassandraCommands.INSERT_INTO,
                                    CassandraTables.TRACKERS,
                                    CassandraTables.TRACKER_FUEL_CONSUMPTION,
                                    super.joinWithAstrix( trackerInfo.getTrackerId() ),
                                    CassandraFunctions.TO_TIMESTAMP.formatted( CassandraFunctions.NOW ),
                                    speed,
                                    ( ( speed * 10 / 36 ) * 15 )
                            ) )
                    );

    // возвпащает данные о машине по намеру или IMEI
    private final BiFunction< String, String, ReqCar > getCarByNumber = ( param, key ) ->
            new ReqCar(
                    this.getSession().execute(
                            MessageFormat.format(
                                    """
                                    {0} {1}.{2} WHERE {3} = {4};
                                    """,
                                    CassandraCommands.SELECT_ALL,
                                    CassandraTables.TABLETS,
                                    CassandraTables.CARS,
                                    param,
                                    super.joinWithAstrix( key )
                            )
                    ).one()
            );

    // возврвщает исторические записи о передвижении машины за определенный период
    private final BiFunction< Request, Boolean, Flux< PositionInfo > > getHistoricalPosition = ( request, flag ) -> Flux.fromStream(
            super.convertRowToStream(
                    this.getSession().execute(
                            MessageFormat.format(
                                    """
                                    {0} {1}.{2}
                                    WHERE imei = {3} AND DATE <= {4} AND date >= {5};
                                    """,
                                    CassandraCommands.SELECT_ALL,
                                    CassandraTables.TRACKERS,
                                    CassandraTables.TRACKERS_LOCATION_TABLE,

                                    super.joinWithAstrix( request.getTrackerId() ),

                                    super.joinWithAstrix( new Date( request.getStartTime().getTime() - 5L * 60 * 60 * 1000 ) ),
                                    super.joinWithAstrix( new Date( request.getEndTime().getTime() - 5L * 60 * 60 * 1000 ) )
                            ) ) ) )
            .parallel( super.checkDifference(
                    (int) Math.abs( Duration.between(
                            request.getStartTime().toInstant(),
                            request.getEndTime().toInstant() ).toDays() ) ) )
            .runOn( Schedulers.parallel() )
            .map( row -> new PositionInfo( row, flag ) )
            .sequential()
            .publishOn( Schedulers.single() );

    /*
    находит данные патрульного по серии паспорта или по уникальному ID
    */
    private final BiFunction< String, Integer, Patrul > getPatrul = ( param, integer ) ->
            new Patrul(
                    this.getSession().execute(
                        MessageFormat.format(
                                """
                                {0} {1}.{2} WHERE {3};
                                """,
                                CassandraCommands.SELECT_ALL,

                                CassandraTables.TABLETS,
                                CassandraTables.PATRULS,

                                ( integer == 0 ? " passportNumber = '" + param + "'" : " uuid = " + param )
                        )
                    ).one()
            );

    public final Function< String, Icons > getPoliceType = policeType -> new Icons(
            this.getSession().execute(
                    MessageFormat.format(
                            """
                            {0} {1}.{2}
                            WHERE policeType = {3};
                            """,
                            CassandraCommands.SELECT_ALL,

                            CassandraTables.TABLETS,
                            CassandraTables.POLICE_TYPE,

                            super.joinWithAstrix( policeType )
                    )
            ).one() );

    private final Function< Boolean, Flux< TrackerInfo > > getAllTrackers = aBoolean -> this.getGetAllEntities()
            .apply( CassandraTables.TRACKERS, CassandraTables.TRACKERSID )
            .filter( row -> !aBoolean || super.check( row ) )
            .map( row -> {
                final ReqCar reqCar = this.getGetCarByNumber().apply( "gosnumber", row.getString( "gosnumber" ) );
                final Patrul patrul = this.getGetPatrul().apply( reqCar.getPatrulPassportSeries(), 0 );
                return new TrackerInfo( patrul, reqCar, row );
            } )
            .sequential()
            .publishOn( Schedulers.single() );

    private final Function< String, Mono< Date > > getLastActiveDate = s -> {
        final Patrul patrul = this.getGetPatrul().apply( s, 1 );

        if ( super.objectIsNotNull( patrul )
                && super.objectIsNotNull( patrul.getUuid() )
                && patrul.getPatrulCarInfo().getCarNumber().compareTo( "null" ) != 0 ) {
            final ReqCar reqCar = this.getGetCarByNumber().apply( "gosnumber", patrul.getPatrulCarInfo().getCarNumber() );
            return super.convert(
                    this.getSession().execute(
                            MessageFormat.format(
                                    """
                                    {0} {1}.{2} WHERE trackersid = {3};
                                    """,
                                    CassandraCommands.SELECT_ALL.replaceAll( "[*]", "lastActiveDate" ),
                                    CassandraTables.TRACKERS,
                                    CassandraTables.TRACKERSID,
                                    super.joinWithAstrix( reqCar.getTrackerId() ) ) )
                            .one()
                            .getTimestamp( "lastactivedate" )
            );
        } else {
            return Mono.empty();
        }
    };

    private Calendar end;
    private Calendar start;

    private final Function< Request, Mono< PatrulFuelStatistics > > calculate_average_fuel_consumption = request ->
            super.convert( new PatrulFuelStatistics() )
                    .map( patrulFuelStatistics -> {
                        final Patrul patrul = this.getGetPatrul().apply( request.getTrackerId(), 1 );
                        if ( !patrul.getPatrulCarInfo().getCarNumber().equals( "null" ) ) {
                            final ReqCar reqCar = this.getGetCarByNumber().apply( "gosnumber", patrul.getPatrulCarInfo().getCarNumber() );
                            this.setStart( super.calendarInstance() );

                            final Row row = this.getSession().execute(
                                            MessageFormat.format(
                                                    """
                                                    {0} {1}.{2}
                                                    WHERE imei = {3} {4};
                                                    """,
                                                    CassandraCommands.SELECT_ALL.replaceAll( "[*]", "min(date) AS min_date, max(date) AS max_date" ),
                                                    CassandraTables.TRACKERS,
                                                    CassandraTables.TRACKER_FUEL_CONSUMPTION,
                                                    super.joinWithAstrix( reqCar.getTrackerId() ),
                                                    ( super.check( request ) ? ""
                                                            : " AND date >= " + super.joinWithAstrix( request.getStartTime() )
                                                            + " AND date <= " + super.joinWithAstrix( request.getEndTime() ) ) ) )
                                    .one();

                            this.getStart().setTime( row.getTimestamp( "min_date" ) );

                            this.setEnd( super.calendarInstance() );

                            this.getEnd().setTime( row.getTimestamp( "max_date" ) );

                            Date date;
                            while ( this.getStart().before( this.getEnd() ) ) {
                                date = this.getStart().getTime();
                                this.getStart().add( Calendar.DATE, 1 );
                                final ConsumptionData consumptionData = new ConsumptionData();

                                consumptionData.setDistance(
                                        this.getSession().execute(
                                                        MessageFormat.format(
                                                                """
                                                                {0} {1}.{2}
                                                                WHERE imei = {3} AND date >= {4} AND date <= {5};
                                                                """,
                                                                CassandraCommands.SELECT_ALL.replaceAll( "[*]", "sum(distance) AS distance_summary" ),
                                                                CassandraTables.TRACKERS,
                                                                CassandraTables.TRACKER_FUEL_CONSUMPTION,
                                                                reqCar.getTrackerId(),
                                                                date.toInstant(),
                                                                this.getStart().toInstant() ) )
                                                .one().getDouble( "distance_summary" ) / 1000 );

                                // переведем метры в километры
                                consumptionData.setFuelLevel( ( consumptionData.getDistance() / 1000 ) /
                                        ( reqCar.getAverageFuelSize() > 0 ? reqCar.getAverageFuelSize() : 10 ) );

                                patrulFuelStatistics.getMap().put( date, consumptionData );

                                patrulFuelStatistics.setAverageFuelConsumption(
                                        patrulFuelStatistics.getAverageFuelConsumption()
                                                + consumptionData.getFuelLevel() );

                                patrulFuelStatistics.setAverageDistance(
                                        patrulFuelStatistics.getAverageDistance()
                                                + consumptionData.getDistance() );
                            }

                            patrulFuelStatistics.setAverageFuelConsumption(
                                    patrulFuelStatistics.getAverageFuelConsumption() / patrulFuelStatistics.getMap().size() );

                            patrulFuelStatistics.setAverageDistance(
                                    patrulFuelStatistics.getAverageDistance() / patrulFuelStatistics.getMap().size() );

                            patrulFuelStatistics.setUuid( patrul.getUuid() );

                        }
                        return patrulFuelStatistics;
                    } )
                    .onErrorReturn( new PatrulFuelStatistics() );

    private final BiFunction< CassandraTables, CassandraTables, ParallelFlux< Row > > getAllEntities =
            ( keyspace, table ) -> Flux.fromStream(
                    super.convertRowToStream(
                            this.getSession().execute( "SELECT * FROM " + keyspace + "." + table + ";" ) ) )
                    .parallel( super.checkDifference( table.name().length() + keyspace.name().length() ) )
                    .runOn( Schedulers.parallel() );

    @Override
    public void close( final Throwable throwable ) {
        super.logging( this );
        this.close();
    }

    @Override
    public void close() {
        instance = null;
        super.logging( this );
        this.getCluster().close();
        this.getSession().close();
        KafkaDataControl.getInstance().close();
    }
}
