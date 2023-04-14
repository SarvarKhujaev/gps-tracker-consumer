package com.ssd.mvd.database;

import java.util.Map;
import java.util.Date;
import java.util.Calendar;

import java.util.function.Consumer;
import java.util.function.Function;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

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
public class CassandraDataControl extends LogInspector {
    private final Cluster cluster;
    private final Session session;
    private static CassandraDataControl instance = new CassandraDataControl();

    public static CassandraDataControl getInstance () { return instance != null ? instance : ( instance = new CassandraDataControl() ); }

    public void register () {
        this.getGetAllTrackers()
                .apply( false )
                .subscribe( trackerInfo -> super.getTrackerInfoMap()
                        .putIfAbsent( trackerInfo.getTrackerId(), trackerInfo ) );

        CassandraDataControlForEscort
                .getInstance()
                .getGetAllTrackers()
                .get()
                .subscribe( trackerInfo -> this.getTupleOfCarMap()
                        .putIfAbsent( trackerInfo.getTrackerId(), trackerInfo ) ); }

    private CassandraDataControl () {
        SocketOptions options = new SocketOptions();
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
                    CassandraTables.TRACKERS.name() +
                    " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy'," +
                    "'datacenter1':3 } AND DURABLE_WRITES = false;" );

        this.getSession().execute("CREATE TABLE IF NOT EXISTS "
                + CassandraTables.TRACKERS.name() + "."
                + CassandraTables.TRACKERSID.name()
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
                + CassandraTables.TRACKERS.name() + "."
                + CassandraTables.TRACKERS_LOCATION_TABLE.name()
                        + "( imei text, " +
                        "date timestamp, " +
                        "speed double, " +
                        "latitude double, " +
                        "longitude double, " +
                        "address text, " +
                "PRIMARY KEY ( (imei), date ) );" );

        this.getSession().execute ( "CREATE TABLE IF NOT EXISTS "
                + CassandraTables.TRACKERS.name() + "."
                + CassandraTables.TRACKER_FUEL_CONSUMPTION.name()
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
        if ( super.getCheckTupleTrackerId().test( position.getDeviceId() ) ) CassandraDataControlForEscort
                .getInstance()
                .getGetTupleOfCarByTracker()
                .apply( position.getDeviceId() )
                .subscribe( tupleOfCar -> { // in case of car exists and in list
                            CassandraDataControlForEscort
                                    .getInstance()
                                    .getSavePosition()
                                    .accept( position );
                            if ( super.getCheckParam().test( tupleOfCar.getUuidOfPatrul() ) ) this.getGetPatrul()
                                    .apply( Map.of( "uuid", tupleOfCar.getUuidOfPatrul().toString() ) )
                                    .subscribe( patrul -> KafkaDataControl
                                            .getInstance()
                                            .getWriteToKafkaEscort()
                                            .accept( super.getTupleOfCarMap()
                                                    .get( position.getDeviceId() )
                                                    .updateTime( position, tupleOfCar, patrul ) ) );
                            else KafkaDataControl
                                    .getInstance()
                                    .getWriteToKafkaEscort()
                                    .accept( super.getTupleOfCarMap()
                                            .get( position.getDeviceId() )
                                            .updateTime( position, tupleOfCar ) ); } );

        else Mono.just( position )
                .flatMap( position1 -> this.getGetCarByNumber().apply( Map.of( "trackerId", position.getDeviceId() ) ) )
                .subscribe( reqCar1 -> {
                    if ( super.getCheckReqCar().test( reqCar1 ) ) {
                        if ( super.getCheckReqCarTrackerId().test( position.getDeviceId() ) ) {
                            // сохраняем в базу только если машина двигается
                            if ( super.getCheckPosition().test( position ) )
                                this.getSession().executeAsync( "INSERT INTO "
                                        + CassandraTables.TRACKERS.name() + "."
                                        + CassandraTables.TRACKERS_LOCATION_TABLE.name()
                                        + "( imei, date, speed, latitude, longitude, address ) "
                                        +  "VALUES ('" + position.getDeviceId()
                                        + "', '" + position.getDeviceTime().toInstant()
                                        + "', " + position.getSpeed()
                                        + ", " + position.getLongitude()
                                        + ", " + position.getLatitude() + ", '' );" );

                            this.getGetPatrul().apply( Map.of( "passportNumber", reqCar1.getPatrulPassportSeries() ) )
                                    .subscribe( patrul -> KafkaDataControl
                                            .getInstance()
                                            .getWriteToKafkaPosition()
                                            .accept( super.getTrackerInfoMap()
                                                    .get( position.getDeviceId() )
                                                    .updateTime( position, reqCar1, patrul ) ) ); }
                        else this.getGetPatrul().apply( Map.of( "passportNumber", reqCar1.getPatrulPassportSeries() ) )
                                .subscribe( patrul -> super.getTrackerInfoMap().put(
                                        reqCar1.getTrackerId(), this.getAddTackerInfo().apply(
                                                new TrackerInfo(
                                                        patrul,
                                                        KafkaDataControl
                                                                .getInstance()
                                                                .getWriteToKafka()
                                                                .apply( reqCar1 ) ) ) ) ); }

                    // в случае если попался не зарешистрированный трекер
                    else super.getUnregisteredTrackers().put( position.getDeviceId(), new Date() ); } );
        return position.getDeviceId(); };

    private final Function< TrackerInfo, TrackerInfo > addTackerInfo = trackerInfo -> {
            this.getSession().execute( "INSERT INTO "
                    + CassandraTables.TRACKERS.name() + "."
                    + CassandraTables.TRACKERSID.name()
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

    public void addValue ( TrackerInfo trackerInfo, Double speed ) {
            if ( speed > 0 ) this.getSession().executeAsync( "INSERT INTO "
                    + CassandraTables.TRACKERS.name() + "."
                    + CassandraTables.TRACKER_FUEL_CONSUMPTION.name()
                    + " ( imei, date, speed, distance ) VALUES('"
                    + trackerInfo.getTrackerId() + "', '"
                    + new Date().toInstant() + "', "
                    + speed + ", "
                    + ( speed * 10 / 36 ) * 15 + ");" ); }

    private final Function< Map< String, String >, Mono< ReqCar > > getCarByNumber = map -> {
            Row row = this.getSession().execute( "SELECT * FROM " +
                    CassandraTables.TABLETS.name() + "." +
                    CassandraTables.CARS.name() +
                    ( map.containsKey( "trackerId" )
                            ? " WHERE trackerId = '" + map.get( "trackerId" )
                            : " WHERE gosnumber = '" + map.get( "gosnumber" ) ) + "';" ).one();
            return Mono.justOrEmpty( super.getCheckParam().test( row ) ? new ReqCar( row ) : null ); };

    private final Function< Request, Flux< PositionInfo > > getHistoricalPosition = request -> Flux.fromStream(
            this.getSession().execute( "SELECT * FROM "
                        + CassandraTables.TRACKERS.name() + "."
                        + CassandraTables.TRACKERS_LOCATION_TABLE.name()
                        + " WHERE imei = '" + request.getTrackerId()
                        + "' AND date >= '" + request.getStartTime().toInstant()
                        + "' AND date <= '" + request.getEndTime().toInstant() + "' ORDER BY date ASC;" )
                    .all()
                    .stream()
                    .parallel() )
            .parallel()
            .runOn( Schedulers.parallel() )
            .map( PositionInfo::new )
            .sequential()
            .publishOn( Schedulers.single() );

    private final Function< Map< String, String >, Mono< Patrul > > getPatrul = map -> Mono.just(
            new Patrul( this.getSession().execute( "SELECT * FROM "
                    + CassandraTables.TABLETS.name() + "."
                    + CassandraTables.PATRULS.name()
                    + ( map.containsKey( "passportNumber" )
                    ? " WHERE passportNumber = '" + map.get( "passportNumber") + "'"
                    : " WHERE uuid = " + map.get( "uuid" ) ) + ";" ).one() ) );

    public final Function< String, Icons > getPoliceType = policeType -> new Icons(
            this.getSession().execute( "SELECT icon, icon2 FROM "
                    + CassandraTables.TABLETS.name() + "."
                    + CassandraTables.POLICE_TYPE.name()
                    + " WHERE policeType = '" + policeType + "';" ).one() );

    private final Function< Boolean, Flux< TrackerInfo > > getAllTrackers = aBoolean -> Flux.fromStream(
            this.getSession().execute( "SELECT * FROM "
                    + CassandraTables.TRACKERS.name() + "."
                    + CassandraTables.TRACKERSID.name() + ";" )
                    .all()
                    .stream()
                    .parallel() )
            .parallel( super.getTrackerInfoMap().size() > 0 ? super.getTrackerInfoMap().size() : 5 )
            .runOn( Schedulers.parallel() )
            .filter( row -> !aBoolean || super.getCheckRow().test( row ) )
            .flatMap( row -> this.getGetCarByNumber().apply( Map.of( "gosnumber", row.getString( "gosnumber" ) ) )
                    .flatMap( reqCar -> this.getGetPatrul().apply( Map.of( "passportNumber", reqCar.getPatrulPassportSeries() ) )
                            .map( patrul -> new TrackerInfo( patrul, reqCar, row ) ) ) )
            .sequential()
            .publishOn( Schedulers.single() );

    private Calendar end;
    private Calendar start;

    private final Function< Request, Mono< PatrulFuelStatistics > > calculate_average_fuel_consumption = request -> {
            final PatrulFuelStatistics patrulFuelStatistics = new PatrulFuelStatistics();
            return this.getGetPatrul().apply( Map.of( "uuid", request.getTrackerId() ) )
                    .flatMap( patrul -> !patrul.getCarNumber().equals( "null" )
                            ? this.getGetCarByNumber().apply( Map.of( "gosnumber", patrul.getCarNumber() ) )
                            .map( reqCar -> {
                                this.setStart( Calendar.getInstance() );
                                this.getStart().setTime( this.getSession()
                                        .execute( "SELECT min(date) AS min_date FROM "
                                                + CassandraTables.TRACKERS.name() + "."
                                                + CassandraTables.TRACKER_FUEL_CONSUMPTION.name()
                                                + " WHERE imei = '" + reqCar.getTrackerId() + "'"
                                                + ( super.getCheckRequest().test( request ) ? ""
                                                : " AND date >= '" + request.getStartTime().toInstant()
                                                + "' AND date <= '" + request.getEndTime().toInstant() + "'" ) + ";" )
                                        .one().getTimestamp( "min_date" ) );

                                this.setEnd( Calendar.getInstance() );
                                this.getEnd().setTime( this.getSession()
                                        .execute( "SELECT max(date) AS max_date FROM "
                                                + CassandraTables.TRACKERS.name() + "."
                                                + CassandraTables.TRACKER_FUEL_CONSUMPTION.name()
                                                + " WHERE imei = '" + reqCar.getTrackerId() + "'"
                                                + ( super.getCheckRequest().test( request ) ? ""
                                                : " AND date >= '" + request.getStartTime().toInstant()
                                                + "' AND date <= '" + request.getEndTime().toInstant() + "'" ) + ";" )
                                        .one().getTimestamp( "max_date" ) );

                                Date date;
                                while ( this.getStart().before( this.getEnd() ) ) {
                                    date = this.getStart().getTime();
                                    this.getStart().add( Calendar.DATE, 1 );
                                    ConsumptionData consumptionData = new ConsumptionData();
                                    consumptionData.setDistance( this.getSession()
                                            .execute( "SELECT sum(distance) AS distance_summary FROM "
                                                    + CassandraTables.TRACKERS.name() + "."
                                                    + CassandraTables.TRACKER_FUEL_CONSUMPTION.name()
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
                                        patrulFuelStatistics.getAverageFuelConsumption() /
                                                patrulFuelStatistics.getMap().size() );
                                patrulFuelStatistics.setAverageDistance(
                                        patrulFuelStatistics.getAverageDistance() /
                                                patrulFuelStatistics.getMap().size() );
                                patrulFuelStatistics.setUuid( patrul.getUuid() );
                                return patrulFuelStatistics; } )
                            .onErrorReturn( new PatrulFuelStatistics() )
                            : Mono.just( patrulFuelStatistics ) ); };
}
