package com.ssd.mvd.database;

import com.ssd.mvd.constants.CassandraTables;
import com.ssd.mvd.kafka.KafkaDataControl;
import com.ssd.mvd.entity.*;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import reactor.core.scheduler.Schedulers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.*;
import java.util.Optional;
import java.util.UUID;
import java.util.List;
import java.util.Map;

@lombok.Data
public final class CassandraDataControlForEscort extends CassandraConverter {
    private final Session session = CassandraDataControl.getInstance().getSession();
    private final Cluster cluster = CassandraDataControl.getInstance().getCluster();

    private static CassandraDataControlForEscort cassandraDataControl = new CassandraDataControlForEscort();

    public static CassandraDataControlForEscort getInstance () { return cassandraDataControl != null
            ? cassandraDataControl : ( cassandraDataControl = new CassandraDataControlForEscort() ); }

    private CassandraDataControlForEscort () {
        this.getSession().execute( "CREATE KEYSPACE IF NOT EXISTS "
                + CassandraTables.ESCORT
                + " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor':3 };" );

        this.getSession().execute("CREATE TABLE IF NOT EXISTS "
                + CassandraTables.ESCORT + "."
                + CassandraTables.TUPLE_OF_CAR +
                super.convertClassToCassandra.apply( TupleOfCar.class ) +
                ", PRIMARY KEY( (uuid), trackerId ) );" ); // the table for cars from Tuple

        this.getSession().execute( "CREATE INDEX IF NOT EXISTS ON "
                + CassandraTables.ESCORT + "."
                + CassandraTables.TUPLE_OF_CAR + " (trackerId);" );

        this.getSession().execute ("CREATE TABLE IF NOT EXISTS "
                + CassandraTables.ESCORT + "."
                + CassandraTables.TRACKERSID
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
                + CassandraTables.ESCORT + "."
                + CassandraTables.ESCORTLOCATION
                + "(imei text, " +
                "date timestamp, " +
                "speed double, " +
                "altitude double, " +
                "longitude double, " +
                "address text, " +
                "PRIMARY KEY ( (imei), date ) );" );

        super.logging( "CassandraDataControlForEscort is ready" ); }

    private final Consumer< Position > savePosition = position ->
            Optional.of( position )
                    .filter( position1 -> super.check.test( position, 6 ) )
                    .ifPresent( position1 -> this.getSession().execute( "INSERT INTO "
                            + CassandraTables.ESCORT + "."
                            + CassandraTables.ESCORTLOCATION
                            + "( imei, date, speed, altitude, longitude, address ) "
                            +  "VALUES ('" + position.getDeviceId()
                            + "', '" + position.getDeviceTime().toInstant()
                            + "', " + position.getSpeed()
                            + ", " + position.getLongitude()
                            + ", " + position.getLatitude()
                            + ", '" + "' );" ) );

    private final Function< TrackerInfo, TrackerInfo > saveTackerInfo = trackerInfo -> {
            this.getSession().execute( ( "INSERT INTO "
                    + CassandraTables.ESCORT + "."
                    + CassandraTables.TRACKERSID
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
            this.getGetCurrentTupleOfCar().apply( tupleOfCar.getUuid() )
                .flatMap( tupleOfCar1 -> {
                    final Optional< TupleOfCar > optional = Optional.of( tupleOfCar );
                    if ( optional.filter( tupleOfCar2 -> !tupleOfCar1.getTrackerId().equals( tupleOfCar.getTrackerId() )
                            && !super.check.test( tupleOfCar.getTrackerId(), 1 ) )
                            .isPresent() )
                        return super.function.apply( Map.of( "message", "Wrong TrackerId", "code", 201 ) );

                    if ( optional.filter( tupleOfCar2 -> super.checkParam.test( tupleOfCar1.getUuidOfPatrul() )
                            && super.checkParam.test( tupleOfCar.getUuidOfPatrul() )
                            && tupleOfCar1.getUuidOfPatrul().compareTo( tupleOfCar.getUuidOfPatrul() ) != 0 )
                            .isPresent() ) {
                        this.getSession().execute ( "UPDATE "
                                + CassandraTables.TABLETS + "."
                                + CassandraTables.PATRULS
                                + " SET uuidforescortcar = " + tupleOfCar.getUuid()
                                + " WHERE uuid = " + tupleOfCar.getUuidOfPatrul() + ";" );

                        this.getSession().execute ( "UPDATE "
                                + CassandraTables.TABLETS + "."
                                + CassandraTables.PATRULS
                                + " SET uuidforescortcar = " + null
                                + " WHERE uuid = " + tupleOfCar1.getUuidOfPatrul() + ";" ); }

                    return this.getSession().execute( "INSERT INTO "
                            + CassandraTables.ESCORT + "."
                            + CassandraTables.TUPLE_OF_CAR
                            + super.getALlNames.apply( TupleOfCar.class )
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
                            ? super.function.apply(
                                    Map.of( "message", "Car" + tupleOfCar.getGosNumber() + " was updated successfully" ) )
                            : super.function.apply(
                                    Map.of( "message", "This car does not exists",
                                            "code", 201,
                                            "success", false ) ); } );

    public void updateEscortCar ( final Double longitude, final Double latitude, final TupleOfCar tupleOfCar ) {
            this.getSession().execute ( "UPDATE "
                    + CassandraTables.ESCORT + "."
                    + CassandraTables.TUPLE_OF_CAR
                    + " SET longitude = " + longitude
                    + ", latitude = " + latitude
                    + " WHERE uuid = " + tupleOfCar.getUuid()
                    + " AND trackerid = '"
                    + tupleOfCar.getTrackerId() + "' IF EXISTS;" ); }

    private final Function< UUID, Mono< TupleOfCar > > getCurrentTupleOfCar = uuid ->
            super.convert( new TupleOfCar( this.getSession().execute( "SELECT * FROM "
                    + CassandraTables.ESCORT + "."
                    + CassandraTables.TUPLE_OF_CAR
                    + " WHERE uuid = " + uuid + ";" ).one() ) );

    private final Function< String, Mono< ApiResponseModel > > deleteTupleOfCar = uuid ->
            this.getGetCurrentTupleOfCar().apply( UUID.fromString( uuid ) )
                    .flatMap( tupleOfCar1 -> !super.checkParam.test( tupleOfCar1.getUuidOfPatrul() )
                            && !super.checkParam.test( tupleOfCar1.getUuidOfEscort() )
                            ? super.function.apply(
                                    Map.of( "message", uuid + " was removed successfully",
                                            "success", this.getSession().execute (
                                                    "DELETE FROM "
                                                            + CassandraTables.ESCORT + "."
                                                            + CassandraTables.TUPLE_OF_CAR
                                                            + " WHERE uuid = " + UUID.fromString( uuid ) + ";" )
                                                    .wasApplied()
                                                    && this.unlinkTupleOfCarFromPatrul.apply( tupleOfCar1.getTrackerId() ) ) )
                            : super.function.apply(
                                    Map.of( "message", "You cannot delete this car, it is linked to Patrul or Escort",
                                            "code", 201,
                                            "success", false ) ) );

    private final Function< TupleOfCar, Mono< ApiResponseModel > > saveNewTupleOfCar = tupleOfCar ->
            super.check.test( tupleOfCar.getTrackerId(), 1 )
            && super.checkCarNumber.test( tupleOfCar.getGosNumber() )
                    ? this.getSession().execute( "INSERT INTO "
                            + CassandraTables.ESCORT + "."
                            + CassandraTables.TUPLE_OF_CAR
                            + super.getALlNames.apply( TupleOfCar.class )
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
                    ? super.checkParam.test( tupleOfCar.getUuidOfPatrul() )
                            ? CassandraDataControl // in case of if this car is linked to patrul
                            .getInstance()
                            .getGetPatrul()
                            .apply( tupleOfCar.getUuidOfPatrul().toString(), 1 )
                            .flatMap( patrul -> {
                                patrul.setCarType( tupleOfCar.getCarModel() );
                                patrul.setCarNumber( tupleOfCar.getGosNumber() );
                                patrul.setUuidForEscortCar( tupleOfCar.getUuid() );
                                this.getSession().execute( "UPDATE "
                                        + CassandraTables.TABLETS + "."
                                        + CassandraTables.PATRULS +
                                        " SET uuidForEscortCar = " + patrul.getUuidForEscortCar()
                                        + ", carType = '" + patrul.getCarType() + "'"
                                        + ", carNumber = '" + patrul.getCarNumber() + "'"
                                        + " WHERE uuid = " + patrul.getUuid() + ";" );
                                return super.function.apply(
                                        Map.of( "message", "Escort was saved successfully",
                                                "success", super.checkParam.test(
                                                        super.tupleOfCarMap.putIfAbsent(
                                                                KafkaDataControl
                                                                .getInstance()
                                                                .getWriteToKafkaTupleOfCar()
                                                                .apply( tupleOfCar )
                                                                .getTrackerId(),
                                                        this.getSaveTackerInfo().apply( new TrackerInfo( patrul, tupleOfCar ) ) ) ) ) ); } )
                            : super.function.apply(
                                    Map.of( "message", "Escort was saved successfully",
                                            "success", super.checkParam.test(
                                                    super.tupleOfCarMap.putIfAbsent(
                                                            tupleOfCar.getTrackerId(),
                                                            this.getSaveTackerInfo().apply( new TrackerInfo( tupleOfCar ) ) ) ) ) )
                    : super.function.apply( Map.of( "message", "This car is already exists", "code", 201 ) )
            : super.function.apply(
                    Map.of( "message", "This trackers or gosnumber is already registered to another car, so choose another one", "code", 201 ) );

    private final Function< String, Mono< TupleOfCar > > getTupleOfCarByTracker = trackersId -> super.convert(
            new TupleOfCar( this.getSession().execute( "SELECT * FROM "
                    + CassandraTables.ESCORT + "."
                    + CassandraTables.TUPLE_OF_CAR
                    + " WHERE trackerId = '" + trackersId + "';" ).one() ) );

    private final Function< String, Mono< TrackerInfo > > getCurrentTracker = trackerId -> super.convert(
            this.getSession().execute( "SELECT * FROM "
                    + CassandraTables.ESCORT + "."
                    + CassandraTables.TRACKERSID
                    + " WHERE trackersId = '" + trackerId + "';" ).one() )
            .flatMap( row -> this.getGetTupleOfCar().apply( row.getString( "gosnumber" ), row.getString( "trackersId" ) )
                    .flatMap( tupleOfCar -> super.checkParam.test( tupleOfCar.getUuidOfPatrul() )
                            ? CassandraDataControl
                                    .getInstance()
                                    .getGetPatrul()
                                    .apply( tupleOfCar.getUuidOfPatrul().toString(), 1 )
                                    .map( patrul -> new TrackerInfo( patrul, tupleOfCar, row ) )
                            : super.convert( new TrackerInfo( tupleOfCar, row ) ) ) );

    private final BiFunction< String, String, Mono< TupleOfCar > > getTupleOfCar = ( gosNumber, trackersId ) -> {
            try { return super.convert( new TupleOfCar(
                    this.getSession().execute( "SELECT * FROM "
                            + CassandraTables.ESCORT + "."
                            + CassandraTables.TUPLE_OF_CAR
                            + " WHERE gosNumber = '" + gosNumber + "';" ).one() ) ); }
            catch ( final Exception e ) {
                super.logging( e.getMessage() );
                this.getSession().execute( "DELETE FROM "
                        + CassandraTables.ESCORT + "."
                        + CassandraTables.TRACKERSID
                        + " WHERE trackersId = '" + trackersId + "';" );
                return Mono.empty(); } };

    private final Supplier< Flux< TrackerInfo > > getAllTrackers = () -> CassandraDataControl
            .getInstance()
            .getGetAllEntities()
            .apply( CassandraTables.ESCORT, CassandraTables.TRACKERSID )
            .flatMap( row -> this.getGetTupleOfCar().apply( row.getString( "gosnumber" ), row.getString( "trackersid" ) )
                    .flatMap( tupleOfCar -> super.checkParam.test( tupleOfCar.getUuidOfPatrul() )
                            ? CassandraDataControl
                                    .getInstance()
                                    .getGetPatrul()
                                    .apply( tupleOfCar.getUuidOfPatrul().toString(), 1 )
                                    .map( patrul -> new TrackerInfo( patrul, tupleOfCar, row ) )
                            : super.convert( new TrackerInfo( tupleOfCar, row ) ) ) )
            .sequential()
            .publishOn( Schedulers.single() );

    private final Function< Point, Flux< TupleOfCar > > findTheClosestCarsInRadius = point -> CassandraDataControl
            .getInstance()
            .getGetAllEntities()
            .apply( CassandraTables.ESCORT, CassandraTables.TUPLE_OF_CAR )
            .filter( row -> super.calculate.apply( point, row ) <= point.getRadius() )
            .map( TupleOfCar::new )
            .sequential()
            .publishOn( Schedulers.single() );

    private final Function< List< Point >, Flux< TupleOfCar > > findTheClosestCarsinPolygon = point -> CassandraDataControl
            .getInstance()
            .getGetAllEntities()
            .apply( CassandraTables.ESCORT, CassandraTables.TUPLE_OF_CAR )
            .filter( row -> super.calculateDistanceInSquare.test( point, row ) )
            .map( TupleOfCar::new )
            .sequential()
            .publishOn( Schedulers.single() );

    private final Function< String, Boolean > unlinkTupleOfCarFromPatrul = s ->
            this.getSession().execute( "DELETE FROM "
                    + CassandraTables.ESCORT + "."
                    + CassandraTables.TRACKERSID
                    + " WHERE trackersid = '" + s + "'"
                    + " IF EXISTS;" ).wasApplied();
}
