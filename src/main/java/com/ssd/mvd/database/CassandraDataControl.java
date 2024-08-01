package com.ssd.mvd.database;

import java.util.Date;
import java.util.Calendar;
import java.text.MessageFormat;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import com.datastax.driver.core.*;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.core.publisher.ParallelFlux;

import com.ssd.mvd.entity.*;
import com.ssd.mvd.inspectors.Inspector;
import com.ssd.mvd.kafka.KafkaDataControl;
import com.ssd.mvd.constants.CassandraTables;
import com.ssd.mvd.constants.CassandraCommands;
import com.ssd.mvd.entity.patrulDataSet.Patrul;
import com.ssd.mvd.subscribers.CustomSubscriber;
import com.ssd.mvd.inspectors.EntitiesInstances;
import com.ssd.mvd.interfaces.ServiceCommonMethods;
import com.ssd.mvd.interfaces.DatabaseCommonMethods;
import com.ssd.mvd.interfaces.EntityToCassandraConverter;
import com.ssd.mvd.interfaces.ObjectFromRowConvertInterface;
import com.ssd.mvd.entity.patrulDataSet.PatrulFuelStatistics;
import com.ssd.mvd.database.cassandraConfigs.CassandraParamsAndOptionsStore;
import com.ssd.mvd.database.cassandraRegistry.CassandraTablesAndTypesRegister;

public final class CassandraDataControl
        extends CassandraParamsAndOptionsStore
        implements DatabaseCommonMethods, ServiceCommonMethods {
    private final Cluster cluster;
    private final Session session;

    private static CassandraDataControl INSTANCE = new CassandraDataControl();

    @Override
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
        /*
        создаем, регистрируем и сохраняем все таблицы, типы и кодеки
        */
        CassandraTablesAndTypesRegister.generate(
                this.getSession()
        );

        this.register();
    }

    private void register () {
        this.getAllEntities
                .apply( EntitiesInstances.POLICE_TYPE )
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
        return INSTANCE != null ? INSTANCE : ( INSTANCE = new CassandraDataControl() );
    }

    private CassandraDataControl () {
        this.session = (
                this.cluster = Cluster
                        .builder()
                        .withPort( super.getPort() )
                        .addContactPoint( super.HOST )
                        .withClusterName( super.CLUSTER_NAME )
                        .withProtocolVersion( ProtocolVersion.V4 )
                        .withRetryPolicy( super.getCustomRetryPolicy() )
                        .withQueryOptions( super.getQueryOptions() )
                        .withSocketOptions( super.getSocketOptions() )
                        .withPoolingOptions( super.getPoolingOptions() )
                        .withCompression( ProtocolOptions.Compression.LZ4 )
                        .build()
        ).connect();

        super.logging( this.getClass() );
    }

    @Override
    public <T> List< Row > getListOfEntities (
            // название таблицы внутри Tablets
            final EntityToCassandraConverter entityToCassandraConverter,
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

                        entityToCassandraConverter.getEntityKeyspaceName(),

                        entityToCassandraConverter.getEntityTableName(),
                        columnName,
                        super.newStringBuilder( "(" ).append( super.convertListToCassandra.apply( ids ) ).append( ")" )
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
                        EntitiesInstances.TUPLE_OF_CAR.generate(
                                this.getRowFromTabletsKeyspace(
                                        EntitiesInstances.TUPLE_OF_CAR,
                                        "trackerId",
                                        super.joinWithAstrix( position.getDeviceId() )
                                )
                        )
                // проверяем что такая машина существует
                ).filter(
                        tupleOfCar -> super.objectIsNotNull( tupleOfCar.getUuid() )
                                && super.objectIsNotNull( tupleOfCar.getGosNumber() )
                ).subscribe(
                        new CustomSubscriber<>(
                                tupleOfCar -> {
                                    /*
                                    проверяем не прикреплена ли машина Эскорта к патрульному
                                    */
                                    final Position updatedPosition = super.objectIsNotNull( tupleOfCar.getUuidOfPatrul() )
                                            ? tupleOfCarMap
                                            .get( position.getDeviceId() )
                                            .updateTime(
                                                    position,
                                                    tupleOfCar,
                                                    EntitiesInstances.PATRUL.generate(
                                                            this.getRowFromTabletsKeyspace(
                                                                    EntitiesInstances.PATRUL,
                                                                    tupleOfCar.getUuidOfPatrul().toString(),
                                                                    "uuid"
                                                            )
                                                    )
                                            )
                                            : tupleOfCarMap
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
                                            .sendMessageToKafka( updatedPosition );
                                }
                        )
                );
            }

            else {
                super.convert(
                        EntitiesInstances.REQ_CAR.generate(
                                this.getRowFromTabletsKeyspace(
                                        EntitiesInstances.REQ_CAR,
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

                                        KafkaDataControl
                                                .getInstance()
                                                .sendMessageToKafka(
                                                        Inspector
                                                                .trackerInfoMap
                                                                .get( optional.get().getDeviceId() )
                                                                .updateTime(
                                                                        optional.get(),
                                                                        reqCar,
                                                                        EntitiesInstances.PATRUL.generate(
                                                                                this.getRowFromTabletsKeyspace(
                                                                                        EntitiesInstances.PATRUL,
                                                                                        reqCar.getPatrulPassportSeries(),
                                                                                        "passportNumber"
                                                                                )
                                                                        )
                                                                )
                                                );
                                    }

                                    // срабатывает когда приходит сигнал от нового трекера на новой машины
                                    else {
                                        final TrackerInfo trackerInfo = new TrackerInfo(
                                                EntitiesInstances.PATRUL.generate(
                                                        this.getRowFromTabletsKeyspace(
                                                                EntitiesInstances.PATRUL,
                                                                reqCar.getPatrulPassportSeries(),
                                                                "passportNumber"
                                                        )
                                                ),
                                                reqCar
                                        );

                                        KafkaDataControl
                                                .getInstance()
                                                .sendMessageToKafka( reqCar );

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
    public final BiFunction< Request, Boolean, Flux< PositionInfo > > getHistoricalPosition = ( request, flag ) -> super.convertValuesToParallelFlux(
                this.getSession().execute(
                        MessageFormat.format(
                                """
                                {0} {1}.{2}
                                WHERE imei = {3} AND date <= {4} AND date >= {5};
                                """,
                                CassandraCommands.SELECT_ALL,

                                EntitiesInstances.POSITION_INFO.getEntityKeyspaceName(),
                                EntitiesInstances.POSITION_INFO.getEntityTableName(),

                                super.joinWithAstrix( request.getTrackerId() ),

                                super.joinWithAstrix( super.newDate( request.getStartTime().getTime() - 5L * 60 * 60 * 1000 ) ),
                                super.joinWithAstrix( super.newDate( request.getEndTime().getTime() - 5L * 60 * 60 * 1000 ) )
                        )
                ),
                super.getTimeDifference(
                        request.getStartTime(),
                        request.getEndTime()
                )
        ).map( row -> new PositionInfo( row, flag ) )
        .sequential()
        .publishOn( Schedulers.single() );

    public final Function< Boolean, Flux< TrackerInfo > > getAllTrackers = aBoolean -> this.getAllEntities
            .apply( EntitiesInstances.TRACKER_INFO )
            .filter( row -> !aBoolean || super.check( row ) )
            .map( row -> {
                final ReqCar reqCar = EntitiesInstances.REQ_CAR.generate(
                        this.getRowFromTabletsKeyspace(
                                EntitiesInstances.REQ_CAR,
                                "gosnumber",
                                row.getString( "gosnumber" )
                        )
                );

                return new TrackerInfo(
                        EntitiesInstances.PATRUL.generate(
                                this.getRowFromTabletsKeyspace(
                                        EntitiesInstances.PATRUL,
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
        final Patrul patrul = EntitiesInstances.PATRUL.generate(
                this.getRowFromTabletsKeyspace(
                        EntitiesInstances.PATRUL,
                        s,
                        "uuid"
                )
        );

        return super.objectIsNotNull( patrul.getUuid() )
                    && patrul.getPatrulCarInfo().getCarNumber().compareTo( "null" ) != 0
                ? super.convert(
                        this.getSession().execute(
                                MessageFormat.format(
                                        """
                                        {0} {1}.{2} WHERE trackersId = {3};
                                        """,
                                        CassandraCommands.SELECT_ALL.replaceAll( "[*]", "lastActiveDate" ),

                                        EntitiesInstances.TRACKER_INFO.getEntityKeyspaceName(),
                                        EntitiesInstances.TRACKER_INFO.getEntityTableName(),

                                        super.joinWithAstrix(
                                                EntitiesInstances.REQ_CAR.generate(
                                                        this.getRowFromTabletsKeyspace(
                                                                EntitiesInstances.REQ_CAR,
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
                        final Patrul patrul = EntitiesInstances.PATRUL.generate(
                                this.getRowFromTabletsKeyspace(
                                        EntitiesInstances.PATRUL,
                                        request.getTrackerId(),
                                        "uuid"
                                )
                        );

                        if ( !patrul.getPatrulCarInfo().getCarNumber().equals( "null" ) ) {
                            final ReqCar reqCar = EntitiesInstances.REQ_CAR.generate(
                                    this.getRowFromTabletsKeyspace(
                                            EntitiesInstances.REQ_CAR,
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

                                                        super.joinWithAstrix( reqCar.getTrackerId() ),
                                                        super.joinWithAstrix( date ),

                                                        super.joinWithAstrix( this.start )
                                                )
                                        ).one().getDouble( "distance_summary" ) / 1000
                                );

                                // переведем метры в километры
                                consumptionData.setFuelLevel(
                                        ( consumptionData.getDistance() / 1000 ) /
                                        ( reqCar.getAverageFuelSize() > 0 ? reqCar.getAverageFuelSize() : 10 )
                                );

                                patrulFuelStatistics.getMap().put( date, consumptionData );

                                patrulFuelStatistics.setAverageFuelConsumption(
                                        patrulFuelStatistics.getAverageFuelConsumption() + consumptionData.getFuelLevel()
                                );

                                patrulFuelStatistics.setAverageDistance(
                                        patrulFuelStatistics.getAverageDistance() + consumptionData.getDistance() );
                            }

                            patrulFuelStatistics.setAverageFuelConsumption(
                                    patrulFuelStatistics.getAverageFuelConsumption() / patrulFuelStatistics.getMap().size()
                            );

                            patrulFuelStatistics.setAverageDistance(
                                    patrulFuelStatistics.getAverageDistance() / patrulFuelStatistics.getMap().size()
                            );

                            patrulFuelStatistics.setUuid( patrul.getUuid() );
                        }

                        return patrulFuelStatistics;
                    } )
                    .onErrorReturn( EntitiesInstances.PATRUL_FUEL_STATISTICS );

    public final Function< EntityToCassandraConverter, ParallelFlux< Row > > getAllEntities =
            entityToCassandraConverter -> super.convertValuesToParallelFlux(
                    this.getSession().execute(
                            entityToCassandraConverter.getSelectAllCommand()
                    ),
                    entityToCassandraConverter.getParallelNumber()
            );

    private <T> T convert (
            final Row row,
            final ObjectFromRowConvertInterface<T> objectFromRowConvertInterface
    ) {
        return new ObjectFromRowConvertInterface<T>() {
            @Override
            public CassandraTables getEntityTableName() {
                return objectFromRowConvertInterface.getEntityTableName();
            }

            @Override
            public CassandraTables getEntityKeyspaceName() {
                return objectFromRowConvertInterface.getEntityKeyspaceName();
            }

            @Override
            public T generate( final Row row ) {
                return objectFromRowConvertInterface.generate( row );
            }

            @Override
            public ObjectFromRowConvertInterface<T> generate() {
                return objectFromRowConvertInterface.generate();
            }
        }.generate().generate( row );
    }

    public <T> Flux< T > getConvertedEntities (
            final ObjectFromRowConvertInterface<T> objectFromRowConvertInterface
    ) {
        return this.getAllEntities
                .apply( objectFromRowConvertInterface )
                .map( row -> this.convert( row, objectFromRowConvertInterface ) )
                .sequential()
                .publishOn( Schedulers.single() );
    }

    public <T> Flux< T > getConvertedEntities (
            final ObjectFromRowConvertInterface<T> objectFromRowConvertInterface,
            final Predicate< Row > checker
    ) {
        return this.getAllEntities
                .apply( objectFromRowConvertInterface )
                .filter( checker )
                .map( row -> this.convert( row, objectFromRowConvertInterface ) )
                .sequential()
                .publishOn( Schedulers.single() );
    }

    @Override
    public void close( final Throwable throwable ) {
        super.logging( this );
        this.close();
    }

    @Override
    public void close() {
        INSTANCE = null;
        super.logging( this );
        this.getCluster().close();
        this.getSession().close();
        KafkaDataControl.getInstance().close();
    }
}
