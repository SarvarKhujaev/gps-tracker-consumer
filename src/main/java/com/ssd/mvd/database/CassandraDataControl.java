package com.ssd.mvd.database;

import java.util.Date;
import java.util.Calendar;
import java.time.Duration;
import java.text.MessageFormat;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.core.publisher.ParallelFlux;

import com.datastax.driver.core.*;

import com.ssd.mvd.entity.*;
import com.ssd.mvd.inspectors.Inspector;
import com.ssd.mvd.kafka.KafkaDataControl;
import com.ssd.mvd.constants.CassandraTables;
import com.ssd.mvd.constants.CassandraCommands;
import com.ssd.mvd.entity.patrulDataSet.Patrul;
import com.ssd.mvd.subscribers.CustomSubscriber;
import com.ssd.mvd.interfaces.ServiceCommonMethods;
import com.ssd.mvd.interfaces.DatabaseCommonMethods;
import com.ssd.mvd.entity.patrulDataSet.PatrulFuelStatistics;
import com.ssd.mvd.database.cassandraConfigs.CassandraParamsAndOptionsStore;

public final class CassandraDataControl
        extends CassandraParamsAndOptionsStore
        implements DatabaseCommonMethods, ServiceCommonMethods {
    private final Cluster cluster;
    private final Session session;

    private static CassandraDataControl instance = new CassandraDataControl();

    public Cluster getCluster() {
        return this.cluster;
    }

    @Override
    public Session getSession() {
        return this.session;
    }

    /*
    создаем, регистрируем и сохраняем все таблицы, типы и кодеки
    */
    public void setCassandraTablesAndTypesRegister () {
        CassandraTablesAndTypesRegister.generate( this.getSession() );
        this.register();
    }

    private void register () {
        this.getAllEntities
                .apply( CassandraTables.TABLETS, CassandraTables.POLICE_TYPE )
                .sequential()
                .publishOn( Schedulers.single() )
                .subscribe( new CustomSubscriber<>( super::save ) );

        this.getAllTrackers.apply( false ).subscribe( new CustomSubscriber<>( super::save ) );

        CassandraDataControlForEscort
                .getInstance()
                .getAllTrackers
                .get()
                .subscribe( new CustomSubscriber<>( super::saveTuple ) );
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
                        super.newStringBuilder( "(" )
                                .append( super.convertListToCassandra.apply( ids ) )
                                .append( ")" )
                )
        ).all();
    }

    private final BiConsumer< Position, TrackerInfo > updateTrackerInfoAndCarLocation = ( updatedPosition, trackerInfo ) ->
            this.getSession().execute(
                    /*
                    запускаем BATCH
                    */
                    super.newStringBuilder()
                            /*
                            сохраняем локацию машин эскорта
                            сохраняются данные всех трекеров
                            */
                            .append( updatedPosition.getEntityUpdateCommand() )
                            /*
                                после получения сигнала от трекера обновляем его значения в БД
                            */
                            .append( trackerInfo.getEntityDeleteCommand() )
                            /*
                            завершаем BATCH
                             */
                            .append( CassandraCommands.APPLY_BATCH )
                            .toString()
            );

    public final Function< Position, String > saveCarLocation = position -> {
            final Optional< Position > optional = Optional.of( position );
            optional.filter( position1 -> !super.check( position.getDeviceId() ) )
                    .ifPresent(
                            position1 -> unregisteredTrackers.put(
                                    position.getDeviceId(),
                                    super.newDate()
                            )
                    );

            /*
            для начала проверяем какому отделу принадлежит машина
            Эскорт или Патрулю
            */
            if ( optional.filter( position1 -> super.check( position.getDeviceId() ) ).isPresent() ) {
                        super.convert(
                                new TupleOfCar(
                                        this.getRowFromTabletsKeyspace(
                                                CassandraTables.TUPLE_OF_CAR,
                                                "trackerId",
                                                super.joinWithAstrix( position.getDeviceId() )
                                        )
                                )
                        // проверяем что такая машина существует
                        ).filter( tupleOfCar -> super.objectIsNotNull( tupleOfCar.getUuid() )
                                && super.objectIsNotNull( tupleOfCar.getGosNumber() ) )
                        .subscribe( new CustomSubscriber<>(
                                tupleOfCar -> {
                                    /*
                                    проверяем не прикреплена ли машина Эскорта к патрульному
                                    */
                                    if ( super.objectIsNotNull( tupleOfCar.getUuidOfPatrul() ) ) {
                                        final Patrul patrul = new Patrul(
                                                this.getRowFromTabletsKeyspace(
                                                        CassandraTables.PATRULS,
                                                        tupleOfCar.getUuidOfPatrul().toString(),
                                                        "uuid"
                                                )
                                        );

                                        /*
                                        обновляем данные самого трекера,
                                        данными машины и патрульного
                                        */
                                        final Position updatedPosition = tupleOfCarMap
                                                .get( position.getDeviceId() )
                                                .updateTime( position, tupleOfCar, patrul );

                                        /*
                                        сохраняем все обновленные данные в БД
                                        */
                                        this.updateTrackerInfoAndCarLocation.accept(
                                                updatedPosition,
                                                tupleOfCarMap.get( position.getDeviceId() )
                                        );

                                        /*
                                            отправляем обновленные данные о позиции машины в Кафку
                                        */
                                        KafkaDataControl
                                                .getInstance()
                                                .writeToKafkaEscort
                                                .accept( updatedPosition );
                                    } else {
                                        /*
                                        обновляем данные самого трекера,
                                        данными машины и патрульного
                                        */
                                        final Position updatedPosition = tupleOfCarMap
                                                .get( position.getDeviceId() )
                                                .updateTime( position, tupleOfCar );

                                        /*
                                        сохраняем все обновленные данные в БД
                                        */
                                        this.updateTrackerInfoAndCarLocation.accept(
                                                updatedPosition,
                                                tupleOfCarMap.get( position.getDeviceId() )
                                        );

                                        /*
                                        отправляем обновленные данные о позиции машины в Кафку
                                        */
                                        KafkaDataControl
                                                .getInstance()
                                                .writeToKafkaEscort
                                                .accept( updatedPosition );
                                    }
                                }
                        ) );
            }

            else {
                super.convert(
                            new ReqCar(
                                    this.getRowFromTabletsKeyspace(
                                            CassandraTables.CARS,
                                            "trackerId",
                                            position.getDeviceId()
                                    )
                            )
                        ).filter( super::check )
                        .subscribe(
                                new CustomSubscriber<>(
                                        reqCar -> {
                                            // убираем уже зарегистрированный трекер
                                            unregisteredTrackers.remove( optional.get().getDeviceId() );

                                            if ( super.check( optional.get().getDeviceId() ) ) {
                                                // сохраняем в базу только если машина двигается
                                                if ( super.check( optional.get() ) ) {
                                                    position.save();
                                                }

                                                final Patrul patrul = new Patrul(
                                                        this.getRowFromTabletsKeyspace(
                                                                CassandraTables.PATRULS,
                                                                reqCar.getPatrulPassportSeries(),
                                                                "passportNumber"
                                                        )
                                                );

                                                KafkaDataControl
                                                        .getInstance()
                                                        .writeToKafkaPosition
                                                        .accept(
                                                                Inspector
                                                                        .trackerInfoMap
                                                                        .get( optional.get().getDeviceId() )
                                                                        .updateTime( optional.get(), reqCar, patrul )
                                                        );
                                            }

                                            // срабатывает когда приходит сигнал от нового трекера на новой машины
                                            else {
                                                final TrackerInfo trackerInfo = new TrackerInfo(
                                                        new Patrul(
                                                                this.getRowFromTabletsKeyspace(
                                                                        CassandraTables.CARS,
                                                                        reqCar.getPatrulPassportSeries(),
                                                                        "passportNumber"
                                                                )
                                                        ),
                                                        reqCar
                                                );

                                                KafkaDataControl
                                                        .getInstance()
                                                        .writeToKafka
                                                        .accept( reqCar );

                                                /*
                                                сохраняет новый трекер
                                                */
                                                trackerInfo.save();

                                                Inspector.trackerInfoMap.put(
                                                        reqCar.getTrackerId(),
                                                        trackerInfo
                                                );
                                            }
                                        }
                                )
                        );
            }

            return position.getDeviceId();
    };

    // возврвщает исторические записи о передвижении машины за определенный период
    public final BiFunction< Request, Boolean, Flux< PositionInfo > > getHistoricalPosition = ( request, flag ) -> Flux.fromStream(
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
                            )
                    )
            ) )
            .parallel( super.checkDifference(
                    (int) Math.abs(
                            Duration.between(
                                    request.getStartTime().toInstant(),
                                    request.getEndTime().toInstant() ).toDays()
                            )
                    )
            ).runOn( Schedulers.parallel() )
            .map( row -> new PositionInfo( row, flag ) )
            .sequential()
            .publishOn( Schedulers.single() );

    public final Function< Boolean, Flux< TrackerInfo > > getAllTrackers = aBoolean -> this.getAllEntities
            .apply( CassandraTables.TRACKERS, CassandraTables.TRACKERSID )
            .filter( row -> !aBoolean || super.check( row ) )
            .map( row -> {
                final ReqCar reqCar = new ReqCar(
                        this.getRowFromTabletsKeyspace( CassandraTables.CARS, "gosnumber", row.getString( "gosnumber" ) )
                );

                return new TrackerInfo(
                        new Patrul(
                                this.getRowFromTabletsKeyspace(
                                        CassandraTables.PATRULS,
                                        reqCar.getPatrulPassportSeries(),
                                        "passportNumber"
                                )
                        ),
                        reqCar,
                        row
                );
            } )
            .sequential()
            .publishOn( Schedulers.single() );

    public final Function< String, Mono< Date > > getLastActiveDate = s -> {
        final Patrul patrul = new Patrul(
                this.getRowFromTabletsKeyspace(
                        CassandraTables.PATRULS,
                        s,
                        "uuid"
                )
        );

        return super.objectIsNotNull( patrul )
                    && super.objectIsNotNull( patrul.getUuid() )
                    && patrul.getPatrulCarInfo().getCarNumber().compareTo( "null" ) != 0
                ? super.convert(
                        this.getSession().execute(
                                MessageFormat.format(
                                        """
                                        {0} {1}.{2} WHERE trackersid = {3};
                                        """,
                                        CassandraCommands.SELECT_ALL.replaceAll( "[*]", "lastActiveDate" ),

                                        CassandraTables.TRACKERS,
                                        CassandraTables.TRACKERSID,

                                        super.joinWithAstrix(
                                                new ReqCar(
                                                        this.getRowFromTabletsKeyspace(
                                                                CassandraTables.CARS,
                                                                patrul.getPatrulCarInfo().getCarNumber(),
                                                                "gosnumber"
                                                        )
                                                ).getTrackerId()
                                        )
                                )
                        ).one().getTimestamp( "lastactivedate" )
                )
                : Mono.empty();
    };

    private Calendar end;
    private Calendar start;

    public final Function< Request, Mono< PatrulFuelStatistics > > calculateAverageFuelConsumption = request ->
            super.convert( new PatrulFuelStatistics() )
                    .map( patrulFuelStatistics -> {
                        final Patrul patrul = new Patrul(
                                this.getRowFromTabletsKeyspace(
                                        CassandraTables.CARS,
                                        request.getTrackerId(),
                                        "uuid"
                                )
                        );

                        if ( !patrul.getPatrulCarInfo().getCarNumber().equals( "null" ) ) {
                            final ReqCar reqCar = new ReqCar(
                                    this.getRowFromTabletsKeyspace(
                                            CassandraTables.CARS,
                                            patrul.getPatrulCarInfo().getCarNumber(),
                                            "gosnumber"
                                    )
                            );

                            this.start = super.calendarInstance();

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
                                            super.check( request )
                                                    ? ""
                                                    : " AND date >= " + super.joinWithAstrix( request.getStartTime() )
                                                    + " AND date <= " + super.joinWithAstrix( request.getEndTime() )
                                    )
                            ).one();

                            this.start.setTime( row.getTimestamp( "min_date" ) );

                            this.end = super.calendarInstance();

                            this.end.setTime( row.getTimestamp( "max_date" ) );

                            Date date;
                            while ( this.start.before( this.end ) ) {
                                date = this.start.getTime();
                                this.start.add( Calendar.DATE, 1 );
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
                                                                this.start.toInstant() ) )
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

    public final BiFunction< CassandraTables, CassandraTables, ParallelFlux< Row > > getAllEntities =
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
