package com.ssd.mvd.database;

import com.ssd.mvd.constants.CassandraTables;
import com.ssd.mvd.kafka.KafkaDataControl;
import com.ssd.mvd.kafka.Inspector;
import com.ssd.mvd.entity.*;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Row;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.logging.Logger;

import java.util.UUID;
import java.util.Map;
import lombok.Data;

@Data
public class CassandraDataControlForEscort {
    private final Session session = CassandraDataControl.getInstance().getSession();
    private final Cluster cluster = CassandraDataControl.getInstance().getCluster();

    private final Logger logger = Logger.getLogger( CassandraDataControl.class.toString() );
    private static CassandraDataControlForEscort cassandraDataControl = new CassandraDataControlForEscort();

    public static CassandraDataControlForEscort getInstance () { return cassandraDataControl != null ? cassandraDataControl
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
                "PRIMARY KEY ( (imei), date ) );" );

        this.logger.info( "CassandraDataControlForEscort is ready" ); }

    public void addValue ( Position position ) {
        if ( position.getSpeed() > 0 ) this.getSession().execute( "INSERT INTO "
                + CassandraTables.ESCORT.name() + "."
                + CassandraTables.ESCORTLOCATION.name()
                + "(imei, date, speed, altitude, longitude) "
                +  "VALUES ('" + position.getDeviceId()
                + "', '" + position.getDeviceTime().toInstant()
                + "', " + position.getSpeed()
                + ", " + position.getLongitude()
                + ", " + position.getLatitude() + ");" ); }

    public TrackerInfo addValue ( TrackerInfo trackerInfo ) {
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
        return trackerInfo; }

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
            " where gosnumber = '" + carNumber + "';" ).one() == null
            && this.getSession().execute( "SELECT * FROM "
            + CassandraTables.TABLETS.name() + "."
            + CassandraTables.CARS.name() +
            " where gosnumber = '" + carNumber + "';" ).one() == null;

    public Mono< ApiResponseModel > updateEscortCar ( TupleOfCar tupleOfCar ) { return this.getAllTupleOfCar( tupleOfCar.getUuid() )
            .flatMap( tupleOfCar1 -> {
                    if ( !tupleOfCar1.getTrackerId().equals( tupleOfCar.getTrackerId() )
                            && !this.checkTracker
                            .test( tupleOfCar.getTrackerId() ) ) return Inspector
                            .getInspector()
                            .getFunction()
                            .apply( Map.of(
                                    "message", "Wrong TrackerId",
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

                    return this.getSession().execute(
                            "INSERT INTO "
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
                            .apply( Map.of(
                                    "message", "Car" + tupleOfCar.getGosNumber()
                                            + " was updated successfully" ) )
                            : Inspector
                            .getInspector()
                            .getFunction()
                            .apply( Map.of(
                                    "message", "This car does not exists",
                                    "code", 201,
                                    "success", false ) ); } ); }

    public void updateEscortCar ( Double longitude, Double latitude, TupleOfCar tupleOfCar ) {
        this.getSession().execute ( "UPDATE "
                + CassandraTables.ESCORT.name() + "."
                + CassandraTables.TUPLE_OF_CAR.name()
                + " SET longitude = " + longitude +
                        ", latitude = " + latitude +
                        " where uuid = " + tupleOfCar.getUuid()
                        + " and trackerid = '"
                        + tupleOfCar.getTrackerId() + "';" ); }

    public Mono< TupleOfCar > getAllTupleOfCar ( UUID uuid ) {
        return Mono.just( this.getSession().execute(
                "SELECT * FROM "
                        + CassandraTables.ESCORT.name() + "."
                        + CassandraTables.TUPLE_OF_CAR.name()
                + " WHERE uuid = " + uuid + ";" ).one() )
                .map( TupleOfCar::new ); }

    public Mono< ApiResponseModel > deleteCar ( String gosNumber ) {
        return this.getAllTupleOfCar( UUID.fromString( gosNumber ) )
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
                                "success", false ) ) ); }

    public Mono< ApiResponseModel > addValue ( TupleOfCar tupleOfCar ) {
        return this.checkTracker.test( tupleOfCar.getTrackerId() )
                && this.checkCarNumber.test( tupleOfCar.getGosNumber() ) ?
                this.getSession().execute(
                "INSERT INTO "
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
                        tupleOfCar.getAverageFuelConsumption() + ") IF NOT EXISTS;" )

                .wasApplied() ?
                    tupleOfCar.getUuidOfPatrul() != null
                            ? CassandraDataControl // in case of if this car is linked to patrul
                            .getInstance()
                            .getPatrul
                            .apply( Map.of( "uuid", tupleOfCar.getUuidOfPatrul().toString() ) )
                            .flatMap( patrul -> Inspector
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
                                                                    .apply( tupleOfCar ).getTrackerId(),
                                                            this.addValue( new TrackerInfo( patrul, tupleOfCar ) ) )
                                                    != null ) ) ) :
                            Inspector
                                    .getInspector()
                                    .getFunction()
                                    .apply( Map.of(
                                            "message", "Escort was saved successfully",
                                            "success", Inspector
                                                    .getInspector()
                                                    .getTupleOfCarMap()
                                                    .putIfAbsent( tupleOfCar.getTrackerId(),
                                                            this.addValue( new TrackerInfo( tupleOfCar ) ) ) != null ) )
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
                        "code", 201 ) ); }

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
            .flatMap( row -> this.getAllTupleOfCar( row.getString( "gosnumber" ), row.getString( "trackersId" ) )
                    .flatMap( tupleOfCar -> tupleOfCar.getUuidOfPatrul() != null ?
                            CassandraDataControl
                                    .getInstance()
                                    .getPatrul
                                    .apply( Map.of( "uuid", tupleOfCar.getUuidOfPatrul().toString() ) )
                                    .flatMap( patrul -> Mono.just( new TrackerInfo( patrul, tupleOfCar, row ) ) )
                            : Mono.just( new TrackerInfo( tupleOfCar, row ) ) ) );

    private Mono< TupleOfCar > getAllTupleOfCar ( String gosNumber, String trackersId ) {
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
            return Mono.empty(); } }

    private final Supplier< Flux< TrackerInfo > > getAllTrackers = () -> Flux.fromStream(
            this.getSession().execute( "SELECT * FROM "
                            + CassandraTables.ESCORT.name() + "."
                            + CassandraTables.TRACKERSID.name() + ";" )
                    .all().stream() )
            .filter( CassandraDataControl
                    .getInstance()
                    .getCheckTrackerTime() )
            .flatMap( row -> this.getAllTupleOfCar( row.getString( "gosnumber" ),
                            row.getString( "trackersid" ) )
                    .flatMap( tupleOfCar -> tupleOfCar.getUuidOfPatrul() != null ?
                            CassandraDataControl
                                    .getInstance()
                                    .getPatrul
                                    .apply( Map.of( "uuid", tupleOfCar.getUuidOfPatrul().toString() ) )
                                    .flatMap( patrul -> Mono.just( new TrackerInfo( patrul, tupleOfCar, row ) ) )
                            : Mono.just( new TrackerInfo( tupleOfCar, row ) ) ) );

    private Supplier< Flux< TupleOfCar > > getAllTupleOfCar = () -> Flux.fromStream( this.getSession().execute(
            "SELECT * FROM "
                    + CassandraTables.ESCORT.name() + "."
                    + CassandraTables.TUPLE_OF_CAR.name() + ";" )
                    .all().stream() )
            .map( TupleOfCar::new );

    public void clear () {
        this.getSession().close();
        this.getCluster().close();
        cassandraDataControl = null;
        this.logger.info( "CassandraDataControlForEscort is closed" ); }
}
