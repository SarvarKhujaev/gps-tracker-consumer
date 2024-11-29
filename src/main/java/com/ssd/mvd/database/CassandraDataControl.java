package com.ssd.mvd.database;

import java.util.Date;
import java.util.Optional;
import java.util.Calendar;

import java.lang.ref.WeakReference;

import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import com.datastax.driver.core.*;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.core.publisher.ParallelFlux;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;

import com.ssd.mvd.entity.*;
import com.ssd.mvd.entity.patrulDataSet.Patrul;
import com.ssd.mvd.entity.patrulDataSet.patrulSubClasses.PatrulFuelStatistics;

import com.ssd.mvd.inspectors.Inspector;
import com.ssd.mvd.inspectors.EntitiesInstances;
import com.ssd.mvd.inspectors.CustomServiceCleaner;
import com.ssd.mvd.inspectors.dataTypesInpectors.TimeInspector;

import com.ssd.mvd.constants.cassandra.CassandraTables;

import com.ssd.mvd.interfaces.DatabaseCommonMethods;
import com.ssd.mvd.interfaces.entity.EntityToCassandraConverter;
import com.ssd.mvd.interfaces.entity.ObjectFromRowConvertInterface;

import com.ssd.mvd.kafka.KafkaDataControl;
import com.ssd.mvd.subscribers.CustomSubscriber;

import com.ssd.mvd.database.cassandraRegistry.CassandraTableRegistration;
import com.ssd.mvd.database.cassandraConfigs.CassandraParamsAndOptionsStore;

@com.ssd.mvd.annotations.services.ImmutableEntityAnnotation
public final class CassandraDataControl extends CassandraParamsAndOptionsStore implements DatabaseCommonMethods {
    private final Cluster cluster;
    private final Session session;

    private static CassandraDataControl INSTANCE = new CassandraDataControl();

    @Override
    @lombok.NonNull
    @lombok.Synchronized
    public synchronized Cluster getCluster() {
        return this.cluster;
    }

    @Override
    @lombok.NonNull
    @lombok.Synchronized
    public synchronized Session getSession() {
        return this.session;
    }

    @SuppressWarnings( value = "создаем, регистрируем и сохраняем все таблицы, типы и кодеки" )
    public void setCassandraTablesAndTypesRegister () {
        /*
        создаем, регистрируем и сохраняем все таблицы, типы и кодеки
        */
        CassandraTableRegistration.generate();

        this.register();
    }

    private void register () {
        this.getAllEntities
                .apply( EntitiesInstances.POLICE_TYPE.get() )
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
        super( CassandraDataControl.class );

        this.session = (
                this.cluster = Cluster
                        .builder()
                        .withPort( CASSANDRA_PORT )
                        .addContactPoint( CASSANDRA_HOST )
                        .withRetryPolicy( CUSTOM_RETRY_POLICY )
                        .withClusterName( CASSANDRA_CLUSTER_NAME )
                        .withCompression( ProtocolOptions.Compression.LZ4 )
                        .withQueryOptions( QUERY_OPTIONS.get() )
//                        .withAuthProvider( AUTH_PROVIDER )
                        .withSocketOptions( SOCKET_OPTIONS.get() )
                        .withCodecRegistry( this.getCodecRegistry() )
                        .withPoolingOptions( super.getPoolingOptions().get() )
                        .withProtocolVersion( ProtocolVersion.V4 )
//                        .withLoadBalancingPolicy( CUSTOM_LOAD_BALANCING )
                        .build()
        ).connect();

        super.logging( this.getClass() );
    }

    private final BiConsumer< Position, TrackerInfo > updateTrackerInfoAndCarLocation = ( updatedPosition, trackerInfo ) ->
        this.completeCommand(
                /*
                запускаем BATCH
                */
                new BatchStatement().add(
                        /*
                        сохраняем локацию машин эскорта
                        сохраняются данные всех трекеров
                        */
                        this.generatePreparedStatement(
                                updatedPosition.getEntityInsert()
                        ).bind()
                ).add(
                        /*
                            после получения сигнала от трекера обновляем его значения в БД
                        */
                        this.generatePreparedStatement(
                                trackerInfo.getEntityInsert()
                        ).bind()
                )
        );

    public final Function< Position, String > saveCarLocation = position -> {
            final Optional< Position > optional = Optional.of( position );
            optional.filter( position1 -> !super.check( position.getDeviceId() ) )
                    .ifPresent(
                            position1 -> unregisteredTrackers.put(
                                    position.getDeviceId(),
                                    newDate()
                            )
                    );

            /*
            для начала проверяем какому отделу принадлежит машина
            Эскорт или Патрулю
            */
            if ( optional.filter( position1 -> super.check( position.getDeviceId() ) ).isPresent() ) {
                super.convert(
                        this.findRowAndReturnEntity(
                                EntitiesInstances.TUPLE_OF_CAR.get(),
                                "trackerId",
                                position.getDeviceId()
                        ).get()
                // проверяем что такая машина существует
                ).filter(
                        tupleOfCar -> super.objectIsNotNull( tupleOfCar.getUuid(), tupleOfCar.getGosNumber() )
                ).subscribe(
                        new CustomSubscriber<>(
                                tupleOfCar -> {
                                    /*
                                    проверяем не прикреплена ли машина Эскорта к патрульному
                                    */
                                    EntitiesInstances.POSITION_ATOMIC_REFERENCE.getAndSet(
                                            super.objectIsNotNull( tupleOfCar.getUuidOfPatrul() )
                                                    ? tupleOfCarMap
                                                    .get( position.getDeviceId() )
                                                    .updateTime(
                                                            position,
                                                            tupleOfCar,
                                                            this.findRowAndReturnEntity(
                                                                    EntitiesInstances.PATRUL.get(),
                                                                    tupleOfCar.getUuidOfPatrul().toString()
                                                            ).get()
                                                    )
                                                    : tupleOfCarMap
                                                    .get( position.getDeviceId() )
                                                    .updateTime( position, tupleOfCar )
                                    );

                                    /*
                                        сохраняем все обновленные данные в БД
                                    */
                                    this.updateTrackerInfoAndCarLocation.accept(
                                            EntitiesInstances.POSITION_ATOMIC_REFERENCE.get(),
                                            tupleOfCarMap.get( position.getDeviceId() )
                                    );

                                    /*
                                        отправляем обновленные данные о позиции машины в Кафку
                                    */
                                    KafkaDataControl
                                            .getKafkaDataControl()
                                            .sendMessageToKafka( EntitiesInstances.POSITION_ATOMIC_REFERENCE.get() );
                                }
                        )
                );
            }

            else {
                super.convert(
                        this.findRowAndReturnEntity(
                                EntitiesInstances.REQ_CAR.get(),
                                "trackerId",
                                joinWithAstrix( position.getDeviceId() )
                        ).get()
                ).filter( super::check )
                .subscribe(
                        new CustomSubscriber<>(
                                reqCar -> {
                                    // убираем уже зарегистрированный трекер
                                    unregisteredTrackers.remove( optional.get().getDeviceId() );

                                    optional.map( position1 -> {
                                                // сохраняем в базу только если машина двигается
                                                if ( check( optional.get() ) ) {
                                                    position.save();
                                                }

                                                KafkaDataControl
                                                        .getKafkaDataControl()
                                                        .sendMessageToKafka(
                                                                Inspector
                                                                        .trackerInfoMap
                                                                        .get( optional.get().getDeviceId() )
                                                                        .updateTime(
                                                                                optional.get(),
                                                                                reqCar,
                                                                                this.findRowAndReturnEntity(
                                                                                        EntitiesInstances.PATRUL.get(),
                                                                                        reqCar.getPatrulPassportSeries(),
                                                                                        getEntityPrimaryKey( EntitiesInstances.PATRUL.get() )[0]
                                                                                ).get()
                                                                        )
                                                        );

                                                return position;
                                            } )
                                            // срабатывает когда приходит сигнал от нового трекера на новой машины
                                            .orElseGet( () -> {
                                                EntitiesInstances.TRACKER_INFO.getAndSet(
                                                        new TrackerInfo(
                                                                this.findRowAndReturnEntity(
                                                                        EntitiesInstances.PATRUL.get(),
                                                                        reqCar.getPatrulPassportSeries(),
                                                                        getEntityPrimaryKey( EntitiesInstances.PATRUL.get() )[0]
                                                                ).get(),
                                                                reqCar
                                                        )
                                                );

                                                KafkaDataControl
                                                        .getKafkaDataControl()
                                                        .sendMessageToKafka( reqCar );

                                                /*
                                                сохраняет новый трекер
                                                */
                                                EntitiesInstances.TRACKER_INFO.get().save();

                                                Inspector.trackerInfoMap.put(
                                                        reqCar.getTrackerId(),
                                                        EntitiesInstances.TRACKER_INFO.get()
                                                );

                                                return position;
                                            } );
                                }
                        )
                );
            }

            return position.getDeviceId();
    };

    @SuppressWarnings(
            value = "возврвщает исторические записи о передвижении машины за определенный период"
    )
    public final BiFunction< Request, Boolean, Flux< PositionInfo > > getHistoricalPosition = ( request, flag ) -> super.convertValuesToParallelFlux(
            this.completeCommand(
                    EntitiesInstances.POSITION_INFO.get().getEntitySelect(
                            request.getTrackerId(),
                            TimeInspector.newDate( request.getStartTime().getTime() - FIVE_HOURS ),
                            TimeInspector.newDate( request.getEndTime().getTime() - FIVE_HOURS )
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
            .apply( EntitiesInstances.TRACKER_INFO.get() )
            .filter( row -> !aBoolean || super.check( row ) )
            .map( row -> {
                EntitiesInstances.REQ_CAR.getAndSet(
                        this.findRowAndReturnEntity(
                                EntitiesInstances.REQ_CAR.get(),
                                "gosnumber",
                                row.getString( "gosnumber" )
                        ).get()
                );

                return new TrackerInfo(
                        this.findRowAndReturnEntity(
                                EntitiesInstances.PATRUL.get(),
                                EntitiesInstances.REQ_CAR.get().getPatrulPassportSeries(),
                                "passportNumber"
                        ).get(),
                        EntitiesInstances.REQ_CAR.get(),
                        row
                );
            } )
            .sequential()
            .publishOn( Schedulers.single() );

    public final Function< String, Mono< Date > > getLastActiveDate = s -> {
        final WeakReference< Patrul > patrul = this.findRowAndReturnEntity(
                EntitiesInstances.PATRUL.get(),
                s
        );

        return super.objectIsNotNull( patrul.get().getUuid() )
                    && patrul.get().getPatrulCarInfo().getCarNumber().compareTo( "null" ) != 0
                ? super.convert(
                        this.completeCommand(
                                EntitiesInstances.TRACKER_INFO.get().getEntitySelect(
                                        this.findRowAndReturnEntity(
                                                EntitiesInstances.REQ_CAR.get(),
                                                patrul.get().getPatrulCarInfo().getCarNumber(),
                                                "gosnumber"
                                        ).get().getTrackerId()
                                )
                        ).one().getTimestamp( "lastactivedate" )
                )
                : Mono.empty();
    };

    public final Function< Request, Mono< PatrulFuelStatistics > > calculateAverageFuelConsumption = request ->
            super.convert( new PatrulFuelStatistics() )
                    .map( patrulFuelStatistics -> {
                        final WeakReference< Patrul > patrul = this.findRowAndReturnEntity(
                                EntitiesInstances.PATRUL.get(),
                                request.getTrackerId()
                        );

                        if ( !patrul.get().getPatrulCarInfo().getCarNumber().equals( "null" ) ) {
                            final WeakReference< ReqCar > reqCar = this.findRowAndReturnEntity(
                                    EntitiesInstances.REQ_CAR.get(),
                                    patrul.get().getPatrulCarInfo().getCarNumber(),
                                    "gosnumber"
                            );

                            TimeInspector.start.getAndSet( super.calendarInstance() );

                            final WeakReference< Row > row = EntitiesInstances.generateWeakEntity(
                                    this.completeCommand(
                                            super.check( request )
                                                    ? EntitiesInstances.PATRUL_FUEL_STATISTICS.get().getEntitySelect(
                                                            reqCar.get().getTrackerId()
                                                    )
                                                    : EntitiesInstances.PATRUL_FUEL_STATISTICS.get().getEntitySelect(
                                                            reqCar.get().getTrackerId(),
                                                            request.getStartTime(),
                                                            request.getEndTime()
                                                    )
                                    ).one()
                            );

                            TimeInspector.start.getAndUpdate( value -> {
                                value.setTime( row.get().getTimestamp( "min_date" ) );
                                return value;
                            } );

                            TimeInspector.end.getAndSet( super.calendarInstance() );

                            TimeInspector.end.getAndUpdate( value -> {
                                value.setTime( row.get().getTimestamp( "max_date" ) );
                                return value;
                            } );

                            Date date;
                            while ( TimeInspector.start.get().before( TimeInspector.end.get() ) ) {
                                date = TimeInspector.start.get().getTime();
                                TimeInspector.start.get().add( Calendar.DATE, 1 );

                                final WeakReference< ConsumptionData > consumptionData = EntitiesInstances.generateWeakEntity(
                                        new ConsumptionData()
                                );

                                consumptionData.get().setDistance(
                                        this.completeCommand(
                                                this.generatePreparedStatement(
                                                        EntitiesInstances.POSITION_ATOMIC_REFERENCE
                                                                .get()
                                                                .startSelect()
                                                                .column(
                                                                        CqlIdentifier.fromCql( "sum(distance)" )
                                                                ).as( CqlIdentifier.fromCql( "distance_summary" ) )
                                                                .where(
                                                                        Relation.column(
                                                                                CqlIdentifier.fromCql( "imei" )
                                                                        ).isEqualTo( QueryBuilder.bindMarker() ),
                                                                        Relation.column(
                                                                                CqlIdentifier.fromCql( "date" )
                                                                        ).isGreaterThanOrEqualTo( QueryBuilder.bindMarker() ),
                                                                        Relation.column(
                                                                                CqlIdentifier.fromCql( "date" )
                                                                        ).isLessThanOrEqualTo( QueryBuilder.bindMarker() )
                                                                )
                                                ).bind(
                                                        QueryBuilder.literal( reqCar.get().getTrackerId() ),
                                                        QueryBuilder.literal( date ),
                                                        QueryBuilder.literal( TimeInspector.start.get() )
                                                )
                                        ).one().getDouble( "distance_summary" ) / 1000
                                );

                                // переведем метры в километры
                                consumptionData.get().setFuelLevel(
                                        ( consumptionData.get().getDistance() / 1000 ) /
                                        ( reqCar.get().getAverageFuelSize() > 0 ? reqCar.get().getAverageFuelSize() : 10 )
                                );

                                patrulFuelStatistics.getMap().put( date, consumptionData.get() );

                                patrulFuelStatistics.setAverageFuelConsumption(
                                        patrulFuelStatistics.getAverageFuelConsumption() + consumptionData.get().getFuelLevel()
                                );

                                patrulFuelStatistics.setAverageDistance(
                                        patrulFuelStatistics.getAverageDistance() + consumptionData.get().getDistance()
                                );

                                clearReference( consumptionData );
                            }

                            patrulFuelStatistics.setAverageFuelConsumption(
                                    patrulFuelStatistics.getAverageFuelConsumption() / patrulFuelStatistics.getMap().size()
                            );

                            patrulFuelStatistics.setAverageDistance(
                                    patrulFuelStatistics.getAverageDistance() / patrulFuelStatistics.getMap().size()
                            );

                            patrulFuelStatistics.setUuid( patrul.get().getUuid() );

                            CustomServiceCleaner.clearReference( reqCar );
                            CustomServiceCleaner.clearReference( row );
                        }

                        CustomServiceCleaner.clearReference( patrul );

                        return patrulFuelStatistics;
                    } )
                    .onErrorReturn( EntitiesInstances.PATRUL_FUEL_STATISTICS.get() );

    public final Function< EntityToCassandraConverter, ParallelFlux< Row > > getAllEntities =
            entityToCassandraConverter -> super.convertValuesToParallelFlux(
                    this.completeCommand( entityToCassandraConverter.getEntitySelect() ),
                    entityToCassandraConverter.getParallelNumber()
            );

    private <T> T convert (
            @lombok.NonNull final Row row,
            @lombok.NonNull final ObjectFromRowConvertInterface<T> objectFromRowConvertInterface
    ) {
        return new ObjectFromRowConvertInterface<T>() {
            @Override
            @lombok.NonNull
            public CassandraTables getEntityTableName() {
                return objectFromRowConvertInterface.getEntityTableName();
            }

            @Override
            @lombok.NonNull
            public CassandraTables getEntityKeyspaceName() {
                return objectFromRowConvertInterface.getEntityKeyspaceName();
            }

            @Override
            @lombok.NonNull
            public T generate( @lombok.NonNull final GettableData row ) {
                return objectFromRowConvertInterface.generate( row );
            }
        }.generate().generate( row );
    }

    public <T> Flux< T > getConvertedEntities (
            @lombok.NonNull final ObjectFromRowConvertInterface<T> objectFromRowConvertInterface
    ) {
        return this.getAllEntities
                .apply( objectFromRowConvertInterface )
                .map( row -> this.convert( row, objectFromRowConvertInterface ) )
                .sequential()
                .publishOn( Schedulers.single() );
    }

    public <T> Flux< T > getConvertedEntities (
            @lombok.NonNull final ObjectFromRowConvertInterface<T> objectFromRowConvertInterface,
            @lombok.NonNull final Predicate< Row > checker
    ) {
        return this.getAllEntities
                .apply( objectFromRowConvertInterface )
                .filter( checker )
                .map( row -> this.convert( row, objectFromRowConvertInterface ) )
                .sequential()
                .publishOn( Schedulers.single() );
    }

    @Override
    public void close( @lombok.NonNull final Throwable throwable ) {
        super.logging( this );
        this.close();
    }

    @Override
    public void close() {
        KafkaDataControl.getKafkaDataControl().close();
        this.getCluster().close();
        this.getSession().close();

        this.clean();
        super.close();

        INSTANCE = null;
        super.logging( this );
    }
}
