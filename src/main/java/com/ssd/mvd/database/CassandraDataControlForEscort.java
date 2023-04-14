package com.ssd.mvd.database;

import com.ssd.mvd.constants.CassandraTables;
import com.ssd.mvd.kafka.KafkaDataControl;
import com.ssd.mvd.entity.*;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Row;

import reactor.core.scheduler.Schedulers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.*;
import java.util.List;
import java.util.UUID;
import java.util.Map;
import lombok.Data;

@Data
public class CassandraDataControlForEscort extends CassandraConverter {
    private final Session session = CassandraDataControl.getInstance().getSession();
    private final Cluster cluster = CassandraDataControl.getInstance().getCluster();

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
                super.convertClassToCassandra( TupleOfCar.class ) +
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

        super.logging( "CassandraDataControlForEscort is ready" ); }

    private final Consumer< Position > savePosition = position -> {
        if ( super.getCheckPosition().test( position ) ) this.getSession().execute( "INSERT INTO "
                + CassandraTables.ESCORT.name() + "."
                + CassandraTables.ESCORTLOCATION.name()
                + "( imei, date, speed, altitude, longitude, address ) "
                +  "VALUES ('" + position.getDeviceId()
                + "', '" + position.getDeviceTime().toInstant()
                + "', " + position.getSpeed()
                + ", " + position.getLongitude()
                + ", " + position.getLatitude()
                + ", '" + "' );" ); };

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

    private final Function< TupleOfCar, Mono< ApiResponseModel > > updateEscortCar = tupleOfCar ->
            this.getGetCurrentTupleOfCar()
                .apply( tupleOfCar.getUuid() )
                .flatMap( tupleOfCar1 -> {
                    if ( !tupleOfCar1.getTrackerId().equals( tupleOfCar.getTrackerId() )
                            && !super.getCheckTracker().test( tupleOfCar.getTrackerId() ) )
                        return super.getFunction().apply( Map.of( "message", "Wrong TrackerId", "code", 201 ) );

                    if ( super.getCheckParam().test( tupleOfCar1.getUuidOfPatrul() )
                            && super.getCheckParam().test( tupleOfCar.getUuidOfPatrul() )
                            && tupleOfCar1.getUuidOfPatrul().compareTo( tupleOfCar.getUuidOfPatrul() ) != 0 ) {
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
                            + super.getALlNames( TupleOfCar.class )
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
                            ? super.getFunction().apply(
                                    Map.of( "message", "Car" + tupleOfCar.getGosNumber() + " was updated successfully" ) )
                            : super.getFunction().apply(
                                    Map.of( "message", "This car does not exists",
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

    private final Function< UUID, Mono< TupleOfCar > > getCurrentTupleOfCar = uuid ->
            Mono.just( new TupleOfCar( this.getSession().execute( "SELECT * FROM "
                            + CassandraTables.ESCORT.name() + "."
                            + CassandraTables.TUPLE_OF_CAR.name()
                            + " WHERE uuid = " + uuid + ";" ).one() ) );

    private final Function< String, Mono< ApiResponseModel > > deleteTupleOfCar = uuid ->
            this.getGetCurrentTupleOfCar().apply( UUID.fromString( uuid ) )
            .flatMap( tupleOfCar1 -> !super.getCheckParam().test( tupleOfCar1.getUuidOfPatrul() )
                    && !super.getCheckParam().test( tupleOfCar1.getUuidOfEscort() )
                    ? super.getFunction().apply(
                            Map.of( "message", uuid + " was removed successfully",
                            "success", this.getSession().execute (
                                    "BEGIN BATCH " +
                                            "DELETE FROM "
                                            + CassandraTables.ESCORT.name() + "."
                                            + CassandraTables.TUPLE_OF_CAR.name()
                                            + " where uuid = " + UUID.fromString( uuid ) + ";" +
                                            " DELETE FROM "
                                            + CassandraTables.ESCORT.name() + "."
                                            + CassandraTables.TRACKERSID.name()
                                            + " WHERE trackersId = '" + tupleOfCar1.getTrackerId() + "';"
                                            + " APPLY BATCH;" )
                                    .wasApplied() ) )
                    : super.getFunction().apply(
                            Map.of( "message", "You cannot delete this car, it is linked to Patrul or Escort",
                            "code", 201,
                            "success", false ) ) );

    private final Function< TupleOfCar, Mono< ApiResponseModel > > saveNewTupleOfCar = tupleOfCar ->
            super.getCheckTracker().test( tupleOfCar.getTrackerId() )
            && super.getCheckCarNumber().test( tupleOfCar.getGosNumber() )
                    ? this.getSession().execute( "INSERT INTO "
                            + CassandraTables.ESCORT.name() + "."
                            + CassandraTables.TUPLE_OF_CAR.name()
                            + super.getALlNames( TupleOfCar.class )
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
                                return super.getFunction().apply(
                                        Map.of( "message", "Escort was saved successfully",
                                                "success", super.getCheckParam().test(
                                                        super.getTupleOfCarMap().putIfAbsent(
                                                        KafkaDataControl
                                                                .getInstance()
                                                                .getWriteToKafkaTupleOfCar()
                                                                .apply( tupleOfCar )
                                                                .getTrackerId(),
                                                        this.getSaveTackerInfo()
                                                                .apply( new TrackerInfo( patrul, tupleOfCar ) ) ) ) ) ); } )
                            : super.getFunction().apply(
                                    Map.of( "message", "Escort was saved successfully",
                                            "success", super.getCheckParam().test(
                            super.getTupleOfCarMap()
                                    .putIfAbsent( tupleOfCar.getTrackerId(),
                                            this.getSaveTackerInfo().apply( new TrackerInfo( tupleOfCar ) ) ) ) ) )
                    : super.getFunction().apply(
                            Map.of( "message", "This car is already exists",
                            "code", 201 ) )
            : super.getFunction().apply(
                    Map.of( "message", "This trackers or gosnumber is already registered to another car, so choose another one",
                    "code", 201 ) );

    private final Function< String, Mono< TupleOfCar > > getTupleOfCarByTracker = trackersId -> {
        final Row row = this.getSession().execute( "SELECT * FROM "
                + CassandraTables.ESCORT.name() + "."
                + CassandraTables.TUPLE_OF_CAR.name()
                + " where trackerId = '" + trackersId + "';" ).one();
        return Mono.justOrEmpty( super.getCheckParam().test( row ) ? new TupleOfCar( row ) : null ); };

    private final Function< String, Mono< TrackerInfo > > getCurrentTracker = trackerId -> Mono.just(
            this.getSession().execute( "SELECT * FROM "
                    + CassandraTables.ESCORT.name() + "."
                    + CassandraTables.TRACKERSID.name()
                    + " where trackersId = '" + trackerId + "';" ).one() )
            .flatMap( row -> this.getGetTupleOfCar()
                    .apply( row.getString( "gosnumber" ), row.getString( "trackersId" ) )
                    .flatMap( tupleOfCar -> super.getCheckParam().test( tupleOfCar.getUuidOfPatrul() )
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
            .flatMap( row -> this.getGetTupleOfCar()
                    .apply( row.getString( "gosnumber" ), row.getString( "trackersid" ) )
                    .flatMap( tupleOfCar -> tupleOfCar.getUuidOfPatrul() != null
                            ? CassandraDataControl
                                    .getInstance()
                                    .getGetPatrul()
                                    .apply( Map.of( "uuid", tupleOfCar.getUuidOfPatrul().toString() ) )
                                    .map( patrul -> new TrackerInfo( patrul, tupleOfCar, row ) )
                            : Mono.just( new TrackerInfo( tupleOfCar, row ) ) ) )
            .sequential()
            .publishOn( Schedulers.single() );

    private Supplier< Flux< TupleOfCar > > getAllTupleOfCar = () -> Flux.fromStream(
            this.getSession().execute( "SELECT * FROM "
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

    private final Function< Point, Flux< TupleOfCar > > findTheClosestCarsInRadius = point -> Flux.fromStream(
            this.getSession().execute( "SELECT * FROM "
                    + CassandraTables.ESCORT.name() + "."
                    + CassandraTables.TUPLE_OF_CAR.name() + ";" )
                    .all()
                    .stream()
                    .parallel() )
            .parallel()
            .runOn( Schedulers.parallel() )
            .filter( tupleOfCar -> super.getCalculate().apply( point, tupleOfCar ) <= point.getRadius() )
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
            .filter( tupleOfCar -> super.getCalculateDistanceInSquare().apply( point, tupleOfCar ) )
            .map( TupleOfCar::new )
            .sequential()
            .publishOn( Schedulers.single() );
}
