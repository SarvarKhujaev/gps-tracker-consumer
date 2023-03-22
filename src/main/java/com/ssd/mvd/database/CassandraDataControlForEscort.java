package com.ssd.mvd.database;

import com.ssd.mvd.controller.UnirestController;
import com.ssd.mvd.constants.CassandraTables;
import com.ssd.mvd.kafka.KafkaDataControl;
import com.ssd.mvd.kafka.Inspector;
import com.ssd.mvd.entity.*;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Row;

import reactor.core.scheduler.Schedulers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.logging.Logger;
import java.util.function.*;
import java.util.List;
import java.util.UUID;
import java.util.Map;
import lombok.Data;

import static java.lang.Math.cos;
import static java.lang.Math.*;

@Data
public class CassandraDataControlForEscort {
    private final Session session = CassandraDataControl.getInstance().getSession();
    private final Cluster cluster = CassandraDataControl.getInstance().getCluster();

    private final Logger logger = Logger.getLogger( CassandraDataControl.class.toString() );
    private static CassandraDataControlForEscort cassandraDataControl = new CassandraDataControlForEscort();

    public static CassandraDataControlForEscort getInstance () { return cassandraDataControl != null
            ? cassandraDataControl
            : ( cassandraDataControl = new CassandraDataControlForEscort() ); }

    private CassandraDataControlForEscort () {
        this.getSession().execute( "CREATE KEYSPACE IF NOT EXISTS "
                + CassandraTables.ESCORT.name()
                + " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor':3 };" );

        this.getSession().execute("CREATE TABLE IF NOT EXISTS "
                + CassandraTables.ESCORT.name() + "."
                + CassandraTables.TUPLE_OF_CAR.name() +
                CassandraConverter
                        .getInstance()
                        .convertClassToCassandra( TupleOfCar.class ) +
                ", PRIMARY KEY( (uuid), trackerId ) );" ); // the table for cars from Tuple

        this.getSession().execute( "CREATE INDEX IF NOT EXISTS ON "
                + CassandraTables.ESCORT.name() + "."
                + CassandraTables.TUPLE_OF_CAR.name() + " (trackerId);" );

        this.getSession().execute ("CREATE TABLE IF NOT EXISTS "
                + CassandraTables.ESCORT.name() + "."
                + CassandraTables.TRACKERSID.name()
                + "( trackersId text PRIMARY KEY, " +
                "patrulPassportSeries text, " +
                "gosnumber text, " +
                "status boolean, " +
                "latitude double, " +
                "longitude double, " +
                "totalActivityTime double, " +
                "lastActiveDate timestamp, " +
                "dateOfRegistration timestamp );" );

        this.getSession().execute ( "CREATE TABLE IF NOT EXISTS "
                + CassandraTables.ESCORT.name() + "."
                + CassandraTables.ESCORTLOCATION.name()
                + "(imei text, " +
                "date timestamp, " +
                "speed double, " +
                "altitude double, " +
                "longitude double, " +
                "address text, " +
                "PRIMARY KEY ( (imei), date ) );" );

        this.logger.info( "CassandraDataControlForEscort is ready" ); }

    private final Consumer< Position > savePosition = position -> {
        if ( CassandraDataControl
                .getInstance()
                .getCheckPosition()
                .test( position ) ) this.getSession().execute( "INSERT INTO "
                + CassandraTables.ESCORT.name() + "."
                + CassandraTables.ESCORTLOCATION.name()
                + "( imei, date, speed, altitude, longitude, address ) "
                +  "VALUES ('" + position.getDeviceId()
                + "', '" + position.getDeviceTime().toInstant()
                + "', " + position.getSpeed()
                + ", " + position.getLongitude()
                + ", " + position.getLatitude()
                + ", '" + UnirestController
                .getInstance()
                .getGetAddressByLocation()
                .apply( position.getLatitude(), position.getLongitude() )
                .replaceAll( "'", "`" ) + "' );" ); };

    private final Function< TrackerInfo, TrackerInfo > saveTackerInfo = trackerInfo -> {
        this.getSession().execute( ( "INSERT INTO "
                + CassandraTables.ESCORT.name() + "."
                + CassandraTables.TRACKERSID.name()
                + "( trackersId, " +
                "patrulPassportSeries, " +
                "gosnumber, " +
                "status, " +
                "latitude, " +
                "longitude, " +
                "totalActivityTime, " +
                "lastActiveDate, " +
                "dateOfRegistration) VALUES('"
                + trackerInfo.getTrackerId() + "', '"
                + trackerInfo.getPatrulPassportSeries() + "', '"
                + trackerInfo.getGosNumber() + "', "
                + trackerInfo.getStatus() + ", "
                + trackerInfo.getLatitude() + ", "
                + trackerInfo.getLongitude() + ", "
                + trackerInfo.getTotalActivityTime() + ", '"
                + trackerInfo.getLastActiveDate().toInstant() + "', '"
                + trackerInfo.getDateOfRegistration().toInstant() + "');" ) );
        return trackerInfo; };

    private final Predicate< String > checkTracker = trackerId -> this.getSession()
            .execute ( "SELECT * FROM "
                    + CassandraTables.ESCORT + "."
                    + CassandraTables.TRACKERSID
                    + " WHERE trackersId = '" + trackerId + "';" ).one() == null
            && this.getSession().execute( "SELECT * FROM "
                    + CassandraTables.TRACKERS + "."
                    + CassandraTables.TRACKERSID
                    + " WHERE trackersId = '" + trackerId + "';" ).one() == null;

    private final Predicate< String > checkCarNumber = carNumber -> this.getSession()
            .execute( "SELECT * FROM "
                    + CassandraTables.ESCORT.name() + "."
                    + CassandraTables.TUPLE_OF_CAR.name() +
                    " WHERE gosnumber = '" + carNumber + "';" ).one() == null
            && this.getSession().execute( "SELECT * FROM "
                    + CassandraTables.TABLETS.name() + "."
                    + CassandraTables.CARS.name() +
                    " WHERE gosnumber = '" + carNumber + "';" ).one() == null;

    private final Function< TupleOfCar, Mono< ApiResponseModel > > updateEscortCar = tupleOfCar ->
            this.getGetCurrentTupleofCar()
                .apply( tupleOfCar.getUuid() )
                .flatMap( tupleOfCar1 -> {
                    if ( !tupleOfCar1.getTrackerId().equals( tupleOfCar.getTrackerId() )
                            && !this.getCheckTracker()
                            .test( tupleOfCar.getTrackerId() ) ) return Inspector
                            .getInspector()
                            .getFunction()
                            .apply( Map.of( "message", "Wrong TrackerId",
                                    "code", 201 ) );

                    if ( tupleOfCar1.getUuidOfPatrul() != null &&
                            tupleOfCar.getUuidOfPatrul() != null &&
                            tupleOfCar1.getUuidOfPatrul().compareTo( tupleOfCar.getUuidOfPatrul() ) != 0 ) {
                        this.getSession().execute ( "UPDATE "
                                + CassandraTables.TABLETS.name() + "."
                                + CassandraTables.PATRULS.name()
                                + " SET uuidforescortcar = " + tupleOfCar.getUuid()
                                + " where uuid = " + tupleOfCar.getUuidOfPatrul() + ";" );

                        this.getSession().execute ( "UPDATE "
                                + CassandraTables.TABLETS.name() + "."
                                + CassandraTables.PATRULS.name()
                                + " SET uuidforescortcar = " + null
                                + " where uuid = " + tupleOfCar1.getUuidOfPatrul() + ";" ); }

                    return this.getSession().execute( "INSERT INTO "
                            + CassandraTables.ESCORT.name() + "."
                            + CassandraTables.TUPLE_OF_CAR.name()
                            + CassandraConverter
                            .getInstance()
                            .getALlNames( TupleOfCar.class )
                            + " VALUES("
                            + tupleOfCar.getUuid() + ", "
                            + tupleOfCar.getUuidOfEscort() + ", "
                            + tupleOfCar.getUuidOfPatrul() + ", '"

                            + tupleOfCar.getCarModel() + "', '"
                            + tupleOfCar.getGosNumber() + "', '"
                            + tupleOfCar.getTrackerId() + "', '"
                            + tupleOfCar.getNsfOfPatrul() + "', '"
                            + tupleOfCar.getSimCardNumber() + "', "

                            + tupleOfCar.getLatitude() + ", "
                            + tupleOfCar.getLongitude() + ", " +
                            tupleOfCar.getAverageFuelConsumption() + " );" )
                            .wasApplied()
                            ? Inspector
                            .getInspector()
                            .getFunction()
                            .apply( Map.of( "message", "Car" + tupleOfCar.getGosNumber()
                                    + " was updated successfully" ) )
                            : Inspector
                            .getInspector()
                            .getFunction()
                            .apply( Map.of(
                                    "message", "This car does not exists",
                                    "code", 201,
                                    "success", false ) ); } );

    public void updateEscortCar ( Double longitude, Double latitude, TupleOfCar tupleOfCar ) {
        this.getSession().execute ( "UPDATE "
                + CassandraTables.ESCORT.name() + "."
                + CassandraTables.TUPLE_OF_CAR.name()
                + " SET longitude = " + longitude
                + ", latitude = " + latitude
                + " where uuid = " + tupleOfCar.getUuid()
                + " and trackerid = '"
                + tupleOfCar.getTrackerId() + "' IF EXISTS;" ); }

    private final Function< UUID, Mono< TupleOfCar > > getCurrentTupleofCar = uuid ->
            Mono.just( this.getSession().execute( "SELECT * FROM "
                    + CassandraTables.ESCORT.name() + "."
                    + CassandraTables.TUPLE_OF_CAR.name()
                    + " WHERE uuid = " + uuid + ";" ).one() )
            .map( TupleOfCar::new );

    private final Function< String, Mono< ApiResponseModel > > deleteTupleOfCar = gosNumber ->
            this.getGetCurrentTupleofCar()
            .apply( UUID.fromString( gosNumber ) )
            .flatMap( tupleOfCar1 -> tupleOfCar1.getUuidOfPatrul() == null
                    && tupleOfCar1.getUuidOfEscort() == null
                    ? Inspector
                    .getInspector()
                    .getFunction()
                    .apply( Map.of(
                            "message", gosNumber + " was removed successfully",
                            "success", this.getSession().execute (
                                    "BEGIN BATCH " +
                                            "DELETE FROM "
                                            + CassandraTables.ESCORT.name() + "."
                                            + CassandraTables.TUPLE_OF_CAR.name()
                                            + " where uuid = " + UUID.fromString( gosNumber ) + ";" +
                                            " DELETE FROM "
                                            + CassandraTables.ESCORT.name() + "."
                                            + CassandraTables.TRACKERSID.name()
                                            + " WHERE trackersId = '" + tupleOfCar1.getTrackerId() + "';"
                                            + " APPLY BATCH;" )
                                    .wasApplied() ) )
                    : Inspector
                    .getInspector()
                    .getFunction()
                    .apply( Map.of(
                            "message", "You cannot delete this car, it is linked to Patrul or Escort",
                            "code", 201,
                            "success", false ) ) );

    private final Function< TupleOfCar, Mono< ApiResponseModel > > saveNewTupleOfCar = tupleOfCar ->
            this.getCheckTracker().test( tupleOfCar.getTrackerId() )
            && this.getCheckCarNumber().test( tupleOfCar.getGosNumber() )
                    ? this.getSession().execute( "INSERT INTO "
                            + CassandraTables.ESCORT.name() + "."
                            + CassandraTables.TUPLE_OF_CAR.name()
                            + CassandraConverter
                            .getInstance()
                            .getALlNames( TupleOfCar.class )
                            + " VALUES("
                            + tupleOfCar.getUuid() + ", "
                            + tupleOfCar.getUuidOfEscort() + ", "
                            + tupleOfCar.getUuidOfPatrul() + ", '"

                            + tupleOfCar.getCarModel() + "', '"
                            + tupleOfCar.getGosNumber() + "', '"
                            + tupleOfCar.getTrackerId() + "', '"
                            + tupleOfCar.getNsfOfPatrul() + "', '"
                            + tupleOfCar.getSimCardNumber() + "', "

                            + tupleOfCar.getLatitude() + ", "
                            + tupleOfCar.getLongitude() + ", "
                            + tupleOfCar.getAverageFuelConsumption() + ") IF NOT EXISTS;" )
                    .wasApplied()
                    ? tupleOfCar.getUuidOfPatrul() != null
                            ? CassandraDataControl // in case of if this car is linked to patrul
                            .getInstance()
                            .getGetPatrul()
                            .apply( Map.of( "uuid", tupleOfCar.getUuidOfPatrul().toString() ) )
                            .flatMap( patrul -> {
                                patrul.setCarType( tupleOfCar.getCarModel() );
                                patrul.setCarNumber( tupleOfCar.getGosNumber() );
                                patrul.setUuidForEscortCar( tupleOfCar.getUuid() );
                                this.getSession().execute( "UPDATE "
                                        + CassandraTables.TABLETS.name() + "."
                                        + CassandraTables.PATRULS +
                                        " SET uuidForEscortCar = " + patrul.getUuidForEscortCar()
                                        + ", carType = '" + patrul.getCarType() + "'"
                                        + ", carNumber = '" + patrul.getCarNumber() + "'"
                                        + " where uuid = " + patrul.getUuid() + ";" );
                                return Inspector
                                        .getInspector()
                                        .getFunction()
                                        .apply( Map.of(
                                                "message", "Escort was saved successfully",
                                                "success", Inspector
                                                        .getInspector()
                                                        .getTupleOfCarMap()
                                                        .putIfAbsent( KafkaDataControl
                                                                        .getInstance()
                                                                        .getWriteToKafkaTupleOfCar()
                                                                        .apply( tupleOfCar )
                                                                        .getTrackerId(),
                                                                this.getSaveTackerInfo()
                                                                        .apply( new TrackerInfo( patrul, tupleOfCar ) ) )
                                                        != null ) ); } )
                            : Inspector
                                    .getInspector()
                                    .getFunction()
                                    .apply( Map.of(
                                            "message", "Escort was saved successfully",
                                            "success", Inspector
                                                    .getInspector()
                                                    .getTupleOfCarMap()
                                                    .putIfAbsent( tupleOfCar.getTrackerId(),
                                                            this.getSaveTackerInfo()
                                                                    .apply( new TrackerInfo( tupleOfCar ) ) ) != null ) )
                    : Inspector
                    .getInspector()
                    .getFunction()
                    .apply( Map.of(
                            "message", "This car is already exists",
                            "code", 201 ) )
            : Inspector
            .getInspector()
            .getFunction()
            .apply( Map.of(
                    "message", "This trackers or gosnumber is already registered to another car, so choose another one",
                    "code", 201 ) );

    private final Function< String, Mono< TupleOfCar > > getTupleOfCarByTracker = trackersId -> {
        Row row = this.getSession().execute( "SELECT * FROM "
                + CassandraTables.ESCORT.name() + "."
                + CassandraTables.TUPLE_OF_CAR.name()
                + " where trackerId = '" + trackersId + "';" ).one();
        return Mono.justOrEmpty( row != null ? new TupleOfCar( row ) : null ); };

    private final Function< String, Mono< TrackerInfo > > getCurrentTracker = trackerId -> Mono.just(
            this.getSession().execute( "SELECT * FROM "
                    + CassandraTables.ESCORT.name() + "."
                    + CassandraTables.TRACKERSID.name()
                    + " where trackersId = '" + trackerId + "';" ).one() )
            .flatMap( row -> this.getGetTupleOfCar()
                    .apply( row.getString( "gosnumber" ), row.getString( "trackersId" ) )
                    .flatMap( tupleOfCar -> tupleOfCar.getUuidOfPatrul() != null
                            ? CassandraDataControl
                                    .getInstance()
                                    .getGetPatrul()
                                    .apply( Map.of( "uuid", tupleOfCar.getUuidOfPatrul().toString() ) )
                                    .flatMap( patrul -> Mono.just( new TrackerInfo( patrul, tupleOfCar, row ) ) )
                            : Mono.just( new TrackerInfo( tupleOfCar, row ) ) ) );

    private final BiFunction< String, String, Mono< TupleOfCar > > getTupleOfCar = ( gosNumber, trackersId ) -> {
        try { return Mono.just( this.getSession().execute( "SELECT * FROM "
                        + CassandraTables.ESCORT.name() + "."
                        + CassandraTables.TUPLE_OF_CAR.name()
                        + " where gosNumber = '" + gosNumber + "';" ).one() )
                .map( TupleOfCar::new );
        } catch ( Exception e ) {
            this.getSession().execute( "DELETE FROM "
                    + CassandraTables.ESCORT.name() + "."
                    + CassandraTables.TRACKERSID.name()
                    + " WHERE trackersId = '" + trackersId + "';" );
            return Mono.empty(); } };

    private final Supplier< Flux< TrackerInfo > > getAllTrackers = () -> Flux.fromStream(
            this.getSession().execute( "SELECT * FROM "
                    + CassandraTables.ESCORT.name() + "."
                    + CassandraTables.TRACKERSID.name() + ";" )
                    .all()
                    .stream()
                    .parallel() )
            .parallel()
            .runOn( Schedulers.parallel() )
//            .filter( CassandraDataControl
//                    .getInstance()
//                    .getCheckTrackerTime() )
            .flatMap( row -> this.getGetTupleOfCar()
                    .apply( row.getString( "gosnumber" ), row.getString( "trackersid" ) )
                    .flatMap( tupleOfCar -> tupleOfCar.getUuidOfPatrul() != null
                            ? CassandraDataControl
                                    .getInstance()
                                    .getGetPatrul()
                                    .apply( Map.of( "uuid", tupleOfCar.getUuidOfPatrul().toString() ) )
                                    .flatMap( patrul -> Mono.just( new TrackerInfo( patrul, tupleOfCar, row ) ) )
                            : Mono.just( new TrackerInfo( tupleOfCar, row ) ) ) )
            .sequential()
            .publishOn( Schedulers.single() );

    private Supplier< Flux< TupleOfCar > > getAllTupleOfCar = () -> Flux.fromStream( this.getSession().execute(
            "SELECT * FROM "
                    + CassandraTables.ESCORT.name() + "."
                    + CassandraTables.TUPLE_OF_CAR.name() + ";" )
            .all()
            .stream()
            .parallel() )
            .parallel()
            .runOn( Schedulers.parallel() )
            .map( TupleOfCar::new )
            .sequential()
            .publishOn( Schedulers.single() );

    private static final Double p = PI / 180;

    private final BiFunction< Point, Row, Double > calculate = ( first, second ) ->
            12742 * asin( sqrt( 0.5 - cos( ( second.getDouble( "latitude" ) - first.getLatitude() ) * p ) / 2
                    + cos( first.getLatitude() * p ) * cos( second.getDouble( "latitude" ) * p )
                    * ( 1 - cos( ( second.getDouble( "longitude" ) - first.getLongitude() ) * p ) ) / 2 ) ) * 1000;

    private final BiFunction< List< Point >, Row, Boolean > calculateDistanceInSquare = ( pointList, row ) -> {
        Boolean result = false;
        int j = pointList.size() - 1;
        for ( int i = 0; i < pointList.size(); i++ ) {
            if ( ( pointList.get( i ).getLatitude() < row.getDouble( "latitude" )
                    && pointList.get( j ).getLatitude() >= row.getDouble( "latitude" )
                    || pointList.get( j ).getLatitude() < row.getDouble( "latitude" )
                    && pointList.get( i ).getLatitude() >= row.getDouble( "latitude" ) )
                    && ( pointList.get( i ).getLongitude() + ( row.getDouble( "latitude" )
                    - pointList.get( i ).getLatitude() ) / ( pointList.get( j ).getLatitude() - pointList.get( j ).getLongitude() )
                    * ( pointList.get( j ).getLatitude() - pointList.get( i ).getLatitude() ) < row.getDouble( "longitude" ) ) )
                result = !result;
            j = i; }
        return result; };

    private final Function< Point, Flux< TupleOfCar > > findTheClosestCarsInRadius = point -> Flux.fromStream(
            this.getSession().execute( "SELECT * FROM "
                    + CassandraTables.ESCORT.name() + "."
                    + CassandraTables.TUPLE_OF_CAR.name() + ";" )
                    .all()
                    .stream()
                    .parallel() )
            .parallel()
            .runOn( Schedulers.parallel() )
            .filter( tupleOfCar -> this.getCalculate().apply( point, tupleOfCar ) <= point.getRadius() )
            .map( TupleOfCar::new )
            .sequential()
            .publishOn( Schedulers.single() );

    private final Function< List< Point >, Flux< TupleOfCar > > findTheClosestCarsinPolygon = point -> Flux.fromStream(
            this.getSession().execute( "SELECT * FROM "
                    + CassandraTables.ESCORT.name() + "."
                    + CassandraTables.TUPLE_OF_CAR.name() + ";" )
                    .all()
                    .stream()
                    .parallel() )
            .parallel()
            .runOn( Schedulers.parallel() )
            .filter( tupleOfCar -> this.getCalculateDistanceInSquare().apply( point, tupleOfCar ) )
            .map( TupleOfCar::new )
            .sequential()
            .publishOn( Schedulers.single() );

    public void clear () {
        this.getSession().close();
        this.getCluster().close();
        cassandraDataControl = null;
        this.logger.info( "CassandraDataControlForEscort is closed" ); }
}
