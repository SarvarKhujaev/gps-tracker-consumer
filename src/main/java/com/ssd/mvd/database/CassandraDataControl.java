package com.ssd.mvd.database;

import lombok.Data;
import java.time.Duration;

import java.util.Map;
import java.util.Date;
import java.util.Calendar;
import java.util.logging.Logger;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.Predicate;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import com.ssd.mvd.entity.*;
import com.ssd.mvd.kafka.Inspector;
import com.ssd.mvd.GpsTrackerApplication;
import com.ssd.mvd.kafka.KafkaDataControl;
import com.ssd.mvd.constants.CassandraTables;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;

@Data
public class CassandraDataControl {
    private final Cluster cluster;
    private final Session session;
    private static CassandraDataControl instance = new CassandraDataControl();
    private final Logger logger = Logger.getLogger( CassandraDataControl.class.toString() );

    public static CassandraDataControl getInstance () { return instance != null ? instance : ( instance = new CassandraDataControl() ); }

    private CassandraDataControl () {
        SocketOptions options = new SocketOptions();
        options.setConnectTimeoutMillis( 30000 );
        options.setReadTimeoutMillis( 300000 );
//        options.setReuseAddress( true );
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
                        "longitude double, PRIMARY KEY ( (imei), date ) );" );

        this.getSession().execute ( "CREATE TABLE IF NOT EXISTS "
                + CassandraTables.TRACKERS.name() + "."
                + CassandraTables.TRACKER_FUEL_CONSUMPTION.name()
                + "( imei text, " +
                "date timestamp, " +
                "speed double, " +
                "distance double, " +
                "PRIMARY KEY ( (imei), date ) );" );

        this.logger.info( "Cassandra is ready" ); }

    private final Consumer< ReqCar > addReqCar = reqCar -> this.getSession()
            .executeAsync( "INSERT INTO "
                    + CassandraTables.TABLETS + "."
                    + CassandraTables.CARS +
            CassandraConverter
                    .getInstance()
                    .getALlNames( ReqCar.class ) +
            " VALUES ("
            + reqCar.getUuid() + ", "
            + reqCar.getLustraId() + ", '"

            + reqCar.getGosNumber() + "', '"
            + reqCar.getTrackerId() + "', '"
            + reqCar.getVehicleType() + "', '"
            + reqCar.getCarImageLink() + "', '"
            + reqCar.getPatrulPassportSeries() + "', "

            + reqCar.getSideNumber() + ", "
            + reqCar.getSimCardNumber() + ", "

            + reqCar.getLatitude() + ", "
            + reqCar.getLongitude() + ", "
            + reqCar.getAverageFuelSize() + ", "
            + reqCar.getAverageFuelConsumption() + ");" );

    private final Function< Position, String > addPosition = position -> {
        if ( Inspector
                .getInspector()
                .getTupleOfCarMap()
                .containsKey( position.getDeviceId() ) ) Mono.just( position )
                .filter( this.getCheckPosition() )
                .map( position1 -> CassandraDataControlForEscort
                        .getInstance()
                        .getGetTupleOfCarByTracker()
                        .apply( position.getDeviceId() ) )
                .subscribe( tupleOfCarMono -> tupleOfCarMono
                        .subscribe( tupleOfCar -> { // in case of car exists and in list
                            CassandraDataControlForEscort
                                    .getInstance()
                                    .addValue( position );
                            if ( tupleOfCar.getUuidOfPatrul() != null ) this.getPatrul
                                    .apply( Map.of( "uuid", tupleOfCar.getUuidOfPatrul().toString() ) )
                                    .subscribe( patrul -> KafkaDataControl
                                            .getInstance()
                                            .getWriteToKafkaEscort()
                                            .accept( Inspector
                                                    .getInspector()
                                                    .getTupleOfCarMap()
                                                    .get( position.getDeviceId() )
                                                    .updateTime( position, tupleOfCar, patrul ) ) );
                            else KafkaDataControl
                                    .getInstance()
                                    .getWriteToKafkaEscort()
                                    .accept( Inspector
                                            .getInspector()
                                            .getTupleOfCarMap()
                                            .get( position.getDeviceId() )
                                            .updateTime( position, tupleOfCar ) ); } ) );

        else Mono.just( position )
                .filter( this.getCheckPosition() )
                .map( position1 -> this.getCarByNumber.apply( Map.of( "trackerId", position.getDeviceId() ) ) )
                .subscribe( reqCarMono -> reqCarMono.subscribe( reqCar1 -> {
                    if ( reqCar1 != null && Inspector
                            .getInspector()
                            .getTrackerInfoMap()
                            .containsKey( position.getDeviceId() ) ) {
                        this.getSession().execute( "INSERT INTO "
                                + CassandraTables.TRACKERS.name() + "."
                                + CassandraTables.TRACKERS_LOCATION_TABLE.name()
                                + "( imei, date, speed, latitude, longitude ) "
                                +  "VALUES ('" + position.getDeviceId()
                                + "', '" + position.getDeviceTime().toInstant()
                                + "', " + position.getSpeed()
                                + ", " + position.getLongitude()
                                + ", " + position.getLatitude() + ");" );
                        this.getPatrul
                                .apply( Map.of( "passportNumber", reqCar1.getPatrulPassportSeries() ) )
                                .subscribe( patrul -> KafkaDataControl
                                        .getInstance()
                                        .getWriteToKafkaPosition()
                                        .accept( Inspector
                                                .getInspector()
                                                .getTrackerInfoMap()
                                                .get( position.getDeviceId() )
                                                .updateTime( position, reqCar1, patrul ) ) ); }

                    else if ( reqCar1 != null && !Inspector
                            .getInspector()
                            .getTrackerInfoMap()
                            .containsKey( position.getDeviceId() ) )
                        this.getPatrul
                                .apply( Map.of( "passportNumber", reqCar1.getPatrulPassportSeries() ) )
                                .subscribe( patrul -> Inspector
                                        .getInspector()
                                        .getTrackerInfoMap()
                                        .put( reqCar1.getTrackerId(),
                                                this.getAddTackerInfo()
                                                        .apply( new TrackerInfo( patrul,
                                                                KafkaDataControl
                                                                .getInstance()
                                                                .getWriteToKafka()
                                                                .apply( reqCar1 ) ) ) ) ); } ) );
        return "success"; };

    private final Function< TrackerInfo, TrackerInfo > addTackerInfo = trackerInfo -> { this.getSession().execute( (
            "INSERT INTO "
                    + CassandraTables.TRACKERS.name() + "."
                    + CassandraTables.TRACKERSID.name()
                    + "(trackersId, " +
                    "patrulPassportSeries, " +
                    "gosnumber, " +
                    "policeType, " +
                    "status, " +
                    "latitude, " +
                    "longitude, " +
                    "totalActivityTime, " +
                    "lastActiveDate, " +
                    "dateOfRegistration ) VALUES('"
                    + trackerInfo.getTrackerId() + "', '"
                    + trackerInfo.getPatrulPassportSeries() + "', '"
                    + trackerInfo.getGosNumber() + "', '"
                    + trackerInfo.getIcon() + "', "
                    + trackerInfo.getStatus() + ", "
                    + trackerInfo.getLatitude() + ", "
                    + trackerInfo.getLongitude() + ", "
                    + trackerInfo.getTotalActivityTime() + ", '"
                    + trackerInfo.getLastActiveDate().toInstant() + "', '"
                    + trackerInfo.getDateOfRegistration().toInstant() + "');" ) );
        return trackerInfo; };

    public void addValue ( TrackerInfo trackerInfo, Double speed ) {
        this.getSession().execute ( "INSERT INTO "
                + CassandraTables.TRACKERS.name() + "."
                + CassandraTables.TRACKER_FUEL_CONSUMPTION.name()
                + " ( imei, date, speed, distance ) VALUES('"
                + trackerInfo.getTrackerId() + "', '"
                + new Date().toInstant() + "', "
                + speed + ", "
                + ( speed * 10 / 36 ) * 15 + ");" ); }

    private final Predicate< Position > checkPosition = position -> position.getLatitude() > 0
            && position.getLongitude() > 0
            && position.getSpeed() > 0
            && position.getDeviceTime()
            .after( new Date( 1605006666774L ) );

    public final Function< Map< String, String >, Mono< ReqCar > > getCarByNumber = map -> {
        Row row = this.getSession().execute( "SELECT * FROM " +
                CassandraTables.TABLETS.name() + "." +
                CassandraTables.CARS.name() +
                ( map.containsKey( "trackerId" )
                        ? " WHERE trackerId = '" + map.get( "trackerId" )
                        : " WHERE gosnumber = '" + map.get( "gosnumber" ) ) + "';" ).one();
        return Mono.justOrEmpty( row != null ? new ReqCar( row ) : null ); };

    public final Function< Request, Flux< PositionInfo > > getHistoricalPosition = request -> Flux.fromStream(
            this.getSession().execute( "SELECT * FROM "
                            + CassandraTables.TRACKERS.name() + "."
                            + CassandraTables.TRACKERS_LOCATION_TABLE.name()
                            + " where imei = '" + request.getTrackerId()
                            + "' and date >= '" + request.getStartTime().toInstant()
                            + "' and date <= '" + request.getEndTime().toInstant() + "';" )
                    .all().stream() )
            .map( PositionInfo::new );

    public Function< Map< String, String >, Mono< Patrul > > getPatrul = map -> Mono.just(
            this.getSession().execute( "SELECT * FROM "
                    + CassandraTables.TABLETS.name() + "."
                    + CassandraTables.PATRULS.name()
                    + ( map.containsKey( "passportNumber" )
                    ? " WHERE passportNumber = '" + map.get( "passportNumber") + "'"
                    : " WHERE uuid = " + map.get( "uuid" ) ) + ";" ).one() )
            .map( Patrul::new );

    public final Function< String, String > getPoliceType = policeType -> {
        Row row = this.getSession().execute(
                "SELECT icon FROM "
                        + CassandraTables.TABLETS.name() + "."
                        + CassandraTables.POLICE_TYPE.name()
                        + " WHERE policeType = '" + policeType + "';" ).one();
        return row != null ? row.getString( "icon" ) : "not found"; };

    private final Predicate< Row > checkTrackerTime = row -> Math.abs( row.getDouble( "totalActivityTime" ) ) > 0;

    public Supplier< Flux< TrackerInfo > > getAllTrackers = () -> Flux.fromStream(
            this.getSession().execute( "SELECT * FROM "
                            + CassandraTables.TRACKERS.name() + "."
                            + CassandraTables.TRACKERSID.name() + ";" )
                    .all().stream() )
            .filter( this.getCheckTrackerTime() )
            .flatMap( row -> this.getCarByNumber
                    .apply( Map.of( "gosnumber", row.getString( "gosnumber" ) ) )
                    .flatMap( reqCar -> this.getPatrul
                            .apply( Map.of( "passportNumber", reqCar.getPatrulPassportSeries() ) )
                            .flatMap( patrul -> Mono.just( new TrackerInfo( patrul, reqCar, row ) ) ) ) );

    private Calendar end;
    private Calendar start;
    private Long timeDifferenceInDays;
    private final Function< Request, Long > getTimeDifferenceInSeconds = request ->
            Math.abs( Duration.between( request.getEndTime().toInstant(),
                            request.getStartTime().toInstant() ).toDays() );

    private final Predicate< Request > checkRequest = request ->
            request.getStartTime() == null
            && request.getEndTime() == null;

    private final Function< Request, Mono< PatrulFuelStatistics > > calculate_average_fuel_consumption = request -> {
        PatrulFuelStatistics patrulFuelStatistics = new PatrulFuelStatistics();
        return this.getPatrul
                .apply( Map.of( "uuid", request.getTrackerId() ) )
                .flatMap( patrul -> !patrul.getCarNumber().equals( "null" )
                        ? this.getCarByNumber
                        .apply( Map.of( "gosnumber", patrul.getCarNumber() ) )
                        .map( reqCar -> {
                            if ( !this.checkRequest.test( request )
                                    && ( this.timeDifferenceInDays = this.getTimeDifferenceInSeconds.apply( request ) ) >= 30 ) {
                                Date date;
                                this.setEnd( Calendar.getInstance() );
                                this.getEnd().setTime( request.getEndTime() );
                                this.setStart( Calendar.getInstance() );
                                this.getStart().setTime( request.getStartTime() );
                                patrulFuelStatistics.setAverageFuelConsumption( this.getSession()
                                        .execute( "SELECT sum(distance) as distance_summary FROM "
                                                + CassandraTables.TRACKERS.name() + "."
                                                + CassandraTables.TRACKER_FUEL_CONSUMPTION.name()
                                                + " where imei = '" + reqCar.getTrackerId() + "'"
                                                + " and date >= '" + request.getStartTime().toInstant()
                                                + "' and date <= '" + request.getEndTime().toInstant() + "';" )
                                        .one().getDouble( "distance_summary" ) / 1000 /
                                        ( reqCar.getAverageFuelSize() > 0 ? reqCar.getAverageFuelSize() : 10 ) /
                                        ( this.getTimeDifferenceInDays() > 0
                                                ? this.getTimeDifferenceInDays() : 1 ) );
                                while ( getStart().before( this.getEnd() ) ) {
                                    date = this.getStart().getTime();
                                    this.getStart().add( Calendar.DATE, 1 );
                                    patrulFuelStatistics
                                            .getMap()
                                            .put( date, new ConsumptionData( 0.0, 0.0 ) ); } }
                            else { this.setStart( Calendar.getInstance() );
                                this.getStart().setTime( this.getSession()
                                        .execute( "SELECT min(date) as min_date FROM "
                                                + CassandraTables.TRACKERS.name() + "."
                                                + CassandraTables.TRACKER_FUEL_CONSUMPTION.name()
                                                + " WHERE imei = '" + reqCar.getTrackerId() + "'"
                                                + ( this.checkRequest.test( request ) ? ""
                                                : " and date >= '" + request.getStartTime().toInstant()
                                                + "' and date <= '" + request.getEndTime().toInstant() + "'" ) + ";" )
                                        .one().getTimestamp( "min_date" ) );

                                this.setEnd( Calendar.getInstance() );
                                this.getEnd().setTime( this.getSession()
                                        .execute( "SELECT max(date) as max_date FROM "
                                                + CassandraTables.TRACKERS.name() + "."
                                                + CassandraTables.TRACKER_FUEL_CONSUMPTION.name()
                                                + " WHERE imei = '" + reqCar.getTrackerId() + "'"
                                                + ( this.checkRequest.test( request ) ? ""
                                                : " and date >= '" + request.getStartTime().toInstant()
                                                + "' and date <= '" + request.getEndTime().toInstant() + "'" ) + ";" )
                                        .one().getTimestamp( "max_date" ) );

                                Date date;
                                while ( getStart().before( this.getEnd() ) ) {
                                    date = this.getStart().getTime();
                                    this.getStart().add( Calendar.DATE, 1 );
                                    ConsumptionData consumptionData = new ConsumptionData();
                                    consumptionData.setDistance( this.getSession()
                                            .execute( "SELECT sum(distance) as distance_summary FROM "
                                                    + CassandraTables.TRACKERS.name() + "."
                                                    + CassandraTables.TRACKER_FUEL_CONSUMPTION.name()
                                                    + " where imei = '" + reqCar.getTrackerId() + "'"
                                                    + " and date >= '" + date.toInstant()
                                                    + "' and date <= '" + this.getStart().toInstant() + "';" )
                                            .one().getDouble( "distance_summary" ) / 1000 );
                                    consumptionData.setFuelLevel( consumptionData.getDistance() /
                                            ( reqCar.getAverageFuelSize() > 0 ? reqCar.getAverageFuelSize() : 10 ) );
                                    patrulFuelStatistics.getMap().put( date, consumptionData ); } }
                            patrulFuelStatistics.setUuid( patrul.getUuid() );
                            return patrulFuelStatistics; } )
                        : Mono.just( patrulFuelStatistics ) ); };

    public void clear () {
        instance = null;
        this.getSession().close();
        this.getCluster().close();
        this.logger.info( "Cassandra is closed" );
        CassandraDataControlForEscort.getInstance().clear(); }
}
