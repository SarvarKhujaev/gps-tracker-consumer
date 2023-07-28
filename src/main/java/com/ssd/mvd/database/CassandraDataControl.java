package com.ssd.mvd.database;

import java.util.Date;
import java.util.Calendar;
import java.time.Duration;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.core.publisher.ParallelFlux;

import com.ssd.mvd.entity.*;
import com.ssd.mvd.GpsTrackerApplication;
import com.ssd.mvd.kafka.KafkaDataControl;
import com.ssd.mvd.inspectors.LogInspector;
import com.ssd.mvd.constants.CassandraTables;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;

@lombok.Data
public final class CassandraDataControl extends LogInspector {
    private final Cluster cluster;
    private final Session session;
    private static CassandraDataControl instance = new CassandraDataControl();

    public static CassandraDataControl getInstance () { return instance != null ? instance : ( instance = new CassandraDataControl() ); }

    public void register () {
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
                .subscribe( trackerInfo -> super.tupleOfCarMap.putIfAbsent( trackerInfo.getTrackerId(), trackerInfo ) ); }

    private CassandraDataControl () {
        final SocketOptions options = new SocketOptions();
        options.setConnectTimeoutMillis( 30000 );
        options.setReadTimeoutMillis( 300000 );
        options.setTcpNoDelay( true );
        options.setKeepAlive( true );
        ( this.session = ( this.cluster = Cluster.builder()
                .withClusterName( GpsTrackerApplication
                        .context
                        .getEnvironment()
                        .getProperty( "variables.KEYSPACE_NAME" ) )
                .addContactPoints( "10.254.5.1, 10.254.5.2, 10.254.5.3".split( ", " ) )
                .withPort( Integer.parseInt( GpsTrackerApplication
                        .context
                        .getEnvironment()
                        .getProperty( "variables.CASSANDRA_PORT" ) ) )
                .withQueryOptions( new QueryOptions()
                        .setConsistencyLevel( ConsistencyLevel.QUORUM )
                        .setDefaultIdempotence( true ) )
                .withRetryPolicy( DefaultRetryPolicy.INSTANCE )
                .withProtocolVersion( ProtocolVersion.V4 )
                .withSocketOptions( options )
                .withLoadBalancingPolicy( new TokenAwarePolicy( DCAwareRoundRobinPolicy.builder().build() ) )
            .withPoolingOptions( new PoolingOptions()
                    .setCoreConnectionsPerHost( HostDistance.REMOTE, Integer.parseInt( GpsTrackerApplication
                            .context
                            .getEnvironment()
                            .getProperty( "variables.CASSANDRA_CORE_CONN_REMOTE" ) ) )
                    .setCoreConnectionsPerHost( HostDistance.LOCAL, Integer.parseInt( GpsTrackerApplication
                            .context
                            .getEnvironment()
                            .getProperty( "variables.CASSANDRA_CORE_CONN_LOCAL" ) ) )
                    .setMaxConnectionsPerHost( HostDistance.REMOTE, Integer.parseInt( GpsTrackerApplication
                            .context
                            .getEnvironment()
                            .getProperty( "variables.CASSANDRA_MAX_CONN_REMOTE" ) ) )
                    .setMaxConnectionsPerHost( HostDistance.LOCAL, Integer.parseInt( GpsTrackerApplication
                            .context
                            .getEnvironment()
                            .getProperty( "variables.CASSANDRA_MAX_CONN_LOCAL" ) ) )
                    .setMaxRequestsPerConnection( HostDistance.REMOTE, 1024 )
                    .setMaxRequestsPerConnection( HostDistance.LOCAL, 1024 )
                    .setPoolTimeoutMillis( 60000 ) ).build() ).connect() )
            .execute( "CREATE KEYSPACE IF NOT EXISTS " +
                    CassandraTables.TRACKERS +
                    " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy'," +
                    "'datacenter1':3 } AND DURABLE_WRITES = false;" );

        this.getSession().execute("CREATE TABLE IF NOT EXISTS "
                + CassandraTables.TRACKERS + "."
                + CassandraTables.TRACKERSID
                + "( trackersId text PRIMARY KEY," +
                "patrulPassportSeries text, " +
                "gosnumber text, " +
                "policeType text, " +
                "status boolean, " +
                "latitude double," +
                "longitude double," +
                "totalActivityTime double, " +
                "lastActiveDate timestamp," +
                "dateOfRegistration timestamp );" );

        this.getSession().execute ( "CREATE TABLE IF NOT EXISTS "
                + CassandraTables.TRACKERS + "."
                + CassandraTables.TRACKERS_LOCATION_TABLE
                        + "( imei text, " +
                        "date timestamp, " +
                        "speed double, " +
                        "latitude double, " +
                        "longitude double, " +
                        "address text, " +
                "PRIMARY KEY ( (imei), date ) );" );

        this.getSession().execute ( "CREATE TABLE IF NOT EXISTS "
                + CassandraTables.TRACKERS + "."
                + CassandraTables.TRACKER_FUEL_CONSUMPTION
                + "( imei text, " +
                "date timestamp, " +
                "speed double, " +
                "distance double, " +
                "PRIMARY KEY ( (imei), date ) );" );

        super.logging( "Cassandra is ready" ); }

    private final Consumer< ReqCar > updateReqCarPosition = reqCar -> this.getSession()
            .executeAsync( "UPDATE "
                    + CassandraTables.TABLETS + "."
                    + CassandraTables.CARS
                    + " SET longitude = " + reqCar.getLongitude()
                    + ", latitude = " + reqCar.getLatitude()
                    + " WHERE uuid = " + reqCar.getUuid() + ";" );

    private final Function< Position, String > addPosition = position -> {
            final Optional< Position > optional = Optional.of( position );
            optional.filter( position1 -> !super.check.test( position.getDeviceId(), 1 )
                            && !super.check.test( position.getDeviceId(), 2 ) )
                    .ifPresent( position1 -> super.unregisteredTrackers.put( position.getDeviceId(), super.getDate.get() ) );

            optional.filter( position1 -> super.check.test( position.getDeviceId(), 1 ) )
                    .map( position1 -> CassandraDataControlForEscort
                            .getInstance()
                            .getGetTupleOfCarByTracker()
                            .apply( position.getDeviceId() )
                            .filter( tupleOfCar -> super.checkParam.test( tupleOfCar.getUuid() )
                                    && super.checkParam.test( tupleOfCar.getGosNumber() ) )
                            .subscribe( tupleOfCar -> { // in case of car exists and in list
                                CassandraDataControlForEscort
                                        .getInstance()
                                        .getSavePosition()
                                        .accept( position );
                                if ( super.checkParam.test( tupleOfCar.getUuidOfPatrul() ) ) this.getGetPatrul()
                                        .apply( tupleOfCar.getUuidOfPatrul().toString(), 1 )
                                        .subscribe( patrul -> KafkaDataControl
                                                .getInstance()
                                                .getWriteToKafkaEscort()
                                                .accept( super.tupleOfCarMap
                                                        .get( position.getDeviceId() )
                                                        .updateTime( position, tupleOfCar, patrul ) ) );
                                else KafkaDataControl
                                        .getInstance()
                                        .getWriteToKafkaEscort()
                                        .accept( super.tupleOfCarMap
                                                .get( position.getDeviceId() )
                                                .updateTime( position, tupleOfCar ) ); } ) )
                    .orElseGet( () -> this.getGetCarByNumber().apply( "trackerId", position.getDeviceId() )
                            .filter( reqCar -> super.check.test( reqCar, 0 ) )
                            .subscribe( reqCar -> {
                                // убираем уже зарегистрированный трекер
                                super.unregisteredTrackers.remove( optional.get().getDeviceId() );
                                if ( super.check.test( optional.get().getDeviceId(), 2 ) ) {
                                    // сохраняем в базу только если машина двигается
                                    if ( super.check.test( optional.get(), 6 ) )
                                        this.getSession().executeAsync( "INSERT INTO "
                                                + CassandraTables.TRACKERS.name() + "."
                                                + CassandraTables.TRACKERS_LOCATION_TABLE.name()
                                                + "( imei, date, speed, latitude, longitude, address ) "
                                                +  "VALUES ('" + optional.get().getDeviceId()
                                                + "', '" + optional.get().getDeviceTime().toInstant()
                                                + "', " + optional.get().getSpeed()
                                                + ", " + optional.get().getLongitude()
                                                + ", " + optional.get().getLatitude() + ", '' );" );

                                    this.getGetPatrul().apply( reqCar.getPatrulPassportSeries(), 0 )
                                            .subscribe( patrul -> KafkaDataControl
                                                    .getInstance()
                                                    .getWriteToKafkaPosition()
                                                    .accept( super.trackerInfoMap.get( optional.get().getDeviceId() )
                                                            .updateTime( optional.get(), reqCar, patrul ) ) ); }
                                // срабатывает когда приходит сигнал от нового трекера на новой машины
                                else this.getGetPatrul().apply( reqCar.getPatrulPassportSeries(), 0 )
                                        .subscribe( patrul -> super.trackerInfoMap.put(
                                                reqCar.getTrackerId(), this.getAddTackerInfo().apply(
                                                        new TrackerInfo(
                                                                patrul,
                                                                KafkaDataControl
                                                                        .getInstance()
                                                                        .getWriteToKafka()
                                                                        .apply( reqCar ) ) ) ) ); } ) );

            return position.getDeviceId(); };

    private final Function< TrackerInfo, TrackerInfo > addTackerInfo = trackerInfo -> {
            this.getSession().execute( "INSERT INTO "
                    + CassandraTables.TRACKERS + "."
                    + CassandraTables.TRACKERSID
                    + "(trackersId, " +
                    "patrulPassportSeries, " +
                    "gosnumber, " +
                    "policeType, " +
                    "policeType2, " +
                    "status, " +
                    "latitude, " +
                    "longitude, " +
                    "totalActivityTime, " +
                    "lastActiveDate, " +
                    "dateOfRegistration ) VALUES('"
                    + trackerInfo.getTrackerId() + "', '"
                    + trackerInfo.getPatrulPassportSeries() + "', '"
                    + trackerInfo.getGosNumber() + "', '"
                    + trackerInfo.getIcon() + "', '"
                    + trackerInfo.getIcon2() + "', "
                    + trackerInfo.getStatus() + ", "
                    + trackerInfo.getLatitude() + ", "
                    + trackerInfo.getLongitude() + ", "
                    + trackerInfo.getTotalActivityTime() + ", '"
                    + trackerInfo.getLastActiveDate().toInstant() + "', '"
                    + trackerInfo.getDateOfRegistration().toInstant() + "');" );
            return trackerInfo; };

    // сохраняем скорость машины и вычислем какое расстояние она проехала после пред сигнала
    private final BiConsumer< TrackerInfo, Double > addValue = ( trackerInfo, speed ) ->
            Optional.of( speed ).filter( speed1 -> speed1 > 0 )
                    .ifPresent( speed1 -> this.getSession().executeAsync( "INSERT INTO "
                            + CassandraTables.TRACKERS + "."
                            + CassandraTables.TRACKER_FUEL_CONSUMPTION
                            + " ( imei, date, speed, distance ) VALUES('"
                            + trackerInfo.getTrackerId() + "', '"
                            + super.getDate.get().toInstant() + "', "
                            + speed + ", "
                            + ( speed * 10 / 36 ) * 15 + ");" ) );

    // возвпащает данные о машине по намеру или IMEI
    private final BiFunction< String, String, Mono< ReqCar > > getCarByNumber = ( param, key ) ->
            super.convert( this.getSession().execute( "SELECT * FROM "
                    + CassandraTables.TABLETS + "."
                    + CassandraTables.CARS
                    + " WHERE " + param + " = '" + key + "';" ).one() )
                    .map( ReqCar::new );

    // возврвщает исторические записи о передвижении машины за определенный период
    private final BiFunction< Request, Boolean, Flux< PositionInfo > > getHistoricalPosition = ( request, flag ) -> Flux.fromStream(
            this.getSession().execute( "SELECT * FROM "
                        + CassandraTables.TRACKERS + "."
                        + CassandraTables.TRACKERS_LOCATION_TABLE
                        + " WHERE imei = '" + request.getTrackerId()
                        + "' AND date >= '" + request.getStartTime().toInstant()
                        + "' AND date <= '" + request.getEndTime().toInstant() + "' ORDER BY date ASC;" )
                    .all()
                    .stream()
                    .parallel() )
            .parallel( super.checkDifference.apply(
                    (int) Math.abs( Duration.between( request.getStartTime().toInstant(), request.getEndTime().toInstant() ).toDays() ) ) )
            .runOn( Schedulers.parallel() )
            .map( row -> new PositionInfo( row, flag ) )
            .sequential()
            .publishOn( Schedulers.single() );

    private final BiFunction< String, Integer, Mono< Patrul > > getPatrul = ( param, integer ) -> super.convert(
            new Patrul( this.getSession().execute( "SELECT * FROM "
                    + CassandraTables.TABLETS + "."
                    + CassandraTables.PATRULS
                    + " WHERE "
                    + ( integer == 0 ? " passportNumber = '" + param + "'" : " uuid = " + param ) + ";" ).one() ) );

    public final Function< String, Icons > getPoliceType = policeType -> new Icons(
            this.getSession().execute( "SELECT icon, icon2 FROM "
                    + CassandraTables.TABLETS + "."
                    + CassandraTables.POLICE_TYPE
                    + " WHERE policeType = '" + policeType + "';" ).one() );

    private final Function< Boolean, Flux< TrackerInfo > > getAllTrackers = aBoolean -> this.getGetAllEntities()
            .apply( CassandraTables.TRACKERS, CassandraTables.TRACKERSID )
            .filter( row -> !aBoolean || super.check.test( row, 4 ) )
            .flatMap( row -> this.getGetCarByNumber().apply( "gosnumber", row.getString( "gosnumber" ) )
                    .flatMap( reqCar -> this.getGetPatrul().apply( reqCar.getPatrulPassportSeries(), 0 )
                            .map( patrul -> new TrackerInfo( patrul, reqCar, row ) ) ) )
            .sequential()
            .publishOn( Schedulers.single() );

    private final Function< String, Mono< Date > > getLastActiveDate = s -> this.getGetPatrul().apply( s, 1 )
            .flatMap( patrul -> super.checkParam.test( patrul )
                    && super.checkParam.test( patrul.getUuid() )
                    && patrul.getCarNumber().compareTo( "null" ) != 0
                    ? this.getGetCarByNumber().apply( "gosnumber", patrul.getCarNumber() )
                            .map( reqCar -> this.getSession().execute(
                                    "SELECT lastactivedate FROM "
                                    + CassandraTables.TRACKERS + "."
                                    + CassandraTables.TRACKERSID
                                    + " WHERE trackersid = '" + reqCar.getTrackerId() + "';" )
                                    .one()
                                    .getTimestamp( "lastactivedate" ) )
                    : Mono.empty() );

    private Calendar end;
    private Calendar start;

    private final Function< Request, Mono< PatrulFuelStatistics > > calculate_average_fuel_consumption = request ->
            super.convert( new PatrulFuelStatistics() )
                    .flatMap( patrulFuelStatistics -> this.getGetPatrul().apply( request.getTrackerId(), 1 )
                            .flatMap( patrul -> !patrul.getCarNumber().equals( "null" )
                                    ? this.getGetCarByNumber().apply( "gosnumber", patrul.getCarNumber() )
                                    .map( reqCar -> {
                                        this.setStart( Calendar.getInstance() );
                                        this.getStart().setTime( this.getSession().execute(
                                                        "SELECT min(date) AS min_date FROM "
                                                                + CassandraTables.TRACKERS + "."
                                                                + CassandraTables.TRACKER_FUEL_CONSUMPTION
                                                                + " WHERE imei = '" + reqCar.getTrackerId() + "'"
                                                                + ( super.check.test( request, 5 )
                                                                ? ""
                                                                : " AND date >= '" + request.getStartTime().toInstant()
                                                                + "' AND date <= '" + request.getEndTime().toInstant() + "'" ) + ";" )
                                                .one()
                                                .getTimestamp( "min_date" ) );

                                        this.setEnd( Calendar.getInstance() );
                                        this.getEnd().setTime( this.getSession().execute(
                                                        "SELECT max(date) AS max_date FROM "
                                                                + CassandraTables.TRACKERS + "."
                                                                + CassandraTables.TRACKER_FUEL_CONSUMPTION
                                                                + " WHERE imei = '" + reqCar.getTrackerId() + "'"
                                                                + ( super.check.test( request, 5 ) ? ""
                                                                : " AND date >= '" + request.getStartTime().toInstant()
                                                                + "' AND date <= '" + request.getEndTime().toInstant() + "'" ) + ";" )
                                                .one()
                                                .getTimestamp( "max_date" ) );

                                        Date date;
                                        while ( this.getStart().before( this.getEnd() ) ) {
                                            date = this.getStart().getTime();
                                            this.getStart().add( Calendar.DATE, 1 );
                                            final ConsumptionData consumptionData = new ConsumptionData();
                                            consumptionData.setDistance( this.getSession()
                                                    .execute( "SELECT sum(distance) AS distance_summary FROM "
                                                            + CassandraTables.TRACKERS + "."
                                                            + CassandraTables.TRACKER_FUEL_CONSUMPTION
                                                            + " WHERE imei = '" + reqCar.getTrackerId() + "'"
                                                            + " AND date >= '" + date.toInstant()
                                                            + "' AND date <= '" + this.getStart().toInstant() + "';" )
                                                    .one().getDouble( "distance_summary" ) / 1000 );
                                            consumptionData.setFuelLevel( consumptionData.getDistance() /
                                                    ( reqCar.getAverageFuelSize() > 0 ? reqCar.getAverageFuelSize() : 10 ) );
                                            patrulFuelStatistics.getMap().put( date, consumptionData );
                                            patrulFuelStatistics.setAverageFuelConsumption(
                                                    patrulFuelStatistics.getAverageFuelConsumption()
                                                            + consumptionData.getFuelLevel() );
                                            patrulFuelStatistics.setAverageDistance(
                                                    patrulFuelStatistics.getAverageDistance()
                                                            + consumptionData.getDistance() ); }
                                        patrulFuelStatistics.setAverageFuelConsumption(
                                                patrulFuelStatistics.getAverageFuelConsumption() / patrulFuelStatistics.getMap().size() );
                                        patrulFuelStatistics.setAverageDistance(
                                                patrulFuelStatistics.getAverageDistance() / patrulFuelStatistics.getMap().size() );
                                        patrulFuelStatistics.setUuid( patrul.getUuid() );
                                        return patrulFuelStatistics; } )
                                    .onErrorReturn( new PatrulFuelStatistics() )
                                    : super.convert( patrulFuelStatistics ) ) );

    private final BiFunction< CassandraTables, CassandraTables, ParallelFlux< Row > > getAllEntities =
            ( keyspace, table ) -> Flux.fromStream(
                    this.getSession().execute( "SELECT * FROM " + keyspace + "." + table + ";" )
                            .all()
                            .stream() )
                    .parallel( super.checkDifference.apply( table.name().length() + keyspace.name().length() ) )
                    .runOn( Schedulers.parallel() );
}
