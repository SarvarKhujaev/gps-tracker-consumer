package com.ssd.mvd.entity;

import com.ssd.mvd.database.CassandraDataControlForEscort;
import com.ssd.mvd.database.CassandraDataControl;
import com.datastax.driver.core.Row;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Data;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TrackerInfo {
    private ReqCar reqCar;
    private Patrul patrul;
    private TupleOfCar tupleOfCar;

    private String icon; // icon
    private String trackerId;
    private String gosNumber;
    private String patrulPassportSeries;

    private Double latitude;
    private Double longitude;

    private Boolean status;
    private Long totalActivityTime;

    private Date lastActiveDate;
    private Date dateOfRegistration = new Date();

    public TrackerInfo ( TupleOfCar tupleOfCar ) {
        this.setStatus( true );
        this.setReqCar( null );
        this.setPatrul( null );
        this.setTotalActivityTime( 0L );
        this.setTupleOfCar( tupleOfCar );
        this.setLastActiveDate( new Date() );
        this.setPatrulPassportSeries( null );
        this.setDateOfRegistration( new Date() );
        this.setGosNumber( tupleOfCar.getGosNumber() );
        this.setTrackerId( tupleOfCar.getTrackerId() ); }

    public TrackerInfo ( Patrul patrul, ReqCar reqCar ) {
        this.setStatus( true );
        this.setTupleOfCar( null );

        this.setPatrul( patrul );
        this.setIcon( CassandraDataControl
                .getInstance()
                .getPoliceType
                .apply( patrul.getPoliceType() ) );
        this.setPatrulPassportSeries( patrul.getPassportNumber() );

        this.setReqCar( reqCar );
        this.setGosNumber( reqCar.getGosNumber() );

        this.setTotalActivityTime( 0L );
        this.setLastActiveDate( new Date() );
        this.setDateOfRegistration( new Date() );
        this.setTrackerId( reqCar.getTrackerId() );
        this.setPatrulPassportSeries( patrul.getPassportNumber() ); }

    public TrackerInfo ( TupleOfCar tupleOfCar, Row row ) {
        this.setTupleOfCar( tupleOfCar );
        this.setStatus( row.getBool( "status" ) );
        this.setGosNumber( tupleOfCar.getGosNumber() );
        this.setLatitude( row.getDouble( "latitude" ) );
        this.setLongitude( row.getDouble( "longitude" ) );
        this.setTrackerId( row.getString( "trackersId" ) );
        this.setLastActiveDate( row.getTimestamp( "lastActiveDate" ) );
        this.setDateOfRegistration( row.getTimestamp( "dateofregistration" ) );
        this.setTotalActivityTime( Math.abs( (long) row.getDouble( "totalActivityTime" ) ) ); }

    public TrackerInfo ( Patrul patrul, TupleOfCar tupleOfCar ) {
        this.setStatus( true );
        this.setReqCar( null );
        this.setPatrul( patrul );
        this.setTotalActivityTime( 0L );
        this.setTupleOfCar( tupleOfCar );
        this.setLastActiveDate( new Date() );
        this.setDateOfRegistration( new Date() );
        this.setGosNumber( tupleOfCar.getGosNumber() );
        this.setTrackerId( tupleOfCar.getTrackerId() );
        this.setPatrulPassportSeries( patrul.getPassportNumber() ); }

    public TrackerInfo ( Patrul patrul, ReqCar reqCar, Row row ) {
        this.setPatrul( patrul );
        this.setReqCar( reqCar );
        this.setGosNumber( reqCar.getGosNumber() );
        this.setStatus( row.getBool( "status" ) );
        this.setLatitude( row.getDouble( "latitude" ) );
        this.setLongitude( row.getDouble( "longitude" ) );
        this.setTrackerId( row.getString( "trackersId" ) );
        this.setPatrulPassportSeries( patrul.getPassportNumber() );
        this.setLastActiveDate( row.getTimestamp( "lastActiveDate" ) );
        this.setDateOfRegistration( row.getTimestamp( "dateofregistration" ) );
        this.setTotalActivityTime( Math.abs( (long) row.getDouble( "totalActivityTime" ) ) ); }

    public TrackerInfo ( Patrul patrul, TupleOfCar tupleOfCar, Row row ) {
        this.setPatrul( patrul );
        this.setTupleOfCar( tupleOfCar );
        this.setStatus( row.getBool( "status" ) );
        this.setGosNumber( tupleOfCar.getGosNumber() );
        this.setLatitude( row.getDouble( "latitude" ) );
        this.setLongitude( row.getDouble( "longitude" ) );
        this.setTrackerId( row.getString( "trackersId" ) );
        this.setPatrulPassportSeries( patrul.getPassportNumber() );
        this.setLastActiveDate( row.getTimestamp( "lastActiveDate" ) );
        this.setDateOfRegistration( row.getTimestamp( "dateofregistration" ) );
        this.setTotalActivityTime( Math.abs( (long) row.getDouble( "totalActivityTime" ) ) ); }

    private Position save ( ReqCar reqCar, Position position ) {
        position.setCarGosNumber( reqCar.getGosNumber() );
        position.setCarType( reqCar.getVehicleType() );

        reqCar.setLongitude( position.getLongitude() );
        reqCar.setLatitude( position.getLatitude() );

        this.setLongitude( position.getLongitude() );
        this.setLatitude( position.getLatitude() );
        this.setGosNumber( reqCar.getGosNumber() );
        this.setReqCar( reqCar );
        CassandraDataControl
                .getInstance()
                .getUpdateReqCarPosition()
                .accept( this.getReqCar() );
        return position; }

    private TrackerInfo save ( Patrul patrul, Position position ) {
        // обновляем позицию патрульного, и трекера
        position.setLongitudeOfTask( patrul.getLongitudeOfTask() );
        position.setLatitudeOfTask( patrul.getLatitudeOfTask() );
        position.setPoliceType( patrul.getPoliceType() );
        position.setPatrulUUID( patrul.getUuid() );
        position.setPatrulName( patrul.getName() );
        position.setStatus( patrul.getStatus() );
        position.setTaskId( patrul.getTaskId() );
        position.setIcon( CassandraDataControl
                .getInstance()
                .getPoliceType
                .apply( patrul.getPoliceType() ) );

        this.setPatrul( patrul );
        this.setIcon( position.getIcon() );
        this.setPatrulPassportSeries( this.getPatrul().getPassportNumber() );
        return this; }

    private TrackerInfo save ( TupleOfCar tupleOfCar, Position position ) {
        // обновляем позицию патрульного, и трекера
        position.setCarGosNumber( tupleOfCar.getGosNumber() );
        position.setCarType( tupleOfCar.getCarModel() );

        tupleOfCar.setLongitude( position.getLongitude() );
        tupleOfCar.setLatitude( position.getLatitude() );

        this.setGosNumber( tupleOfCar.getGosNumber() );
        this.setLongitude( position.getLongitude() );
        this.setLatitude( position.getLatitude() );
        this.setTupleOfCar( tupleOfCar );
        CassandraDataControlForEscort
                .getInstance()
                .updateEscortCar( this.getTupleOfCar().getLongitude(),
                        this.getTupleOfCar().getLatitude(),
                        this.getTupleOfCar() );
        return this; }

    public Position updateTime ( Position position, TupleOfCar tupleOfCar ) {
        this.setPatrul( null );
        this.setPatrulPassportSeries( null );
        this.setLastActiveDate( new Date() );
        this.setTotalActivityTime( Math.abs( this.getTotalActivityTime()
                        + Duration.between( Instant.now(),
                        this.getLastActiveDate()
                                .toInstant() ).getSeconds() ) );
        CassandraDataControlForEscort
                .getInstance()
                .getSaveTackerInfo()
                .apply( this.save( tupleOfCar, position ) );
        return position; }

    public Position updateTime ( Position position, ReqCar reqCar, Patrul patrul ) {
        CassandraDataControl
                .getInstance()
                .addValue( this, position.getSpeed() );
        this.setLastActiveDate( new Date() );
        this.setTotalActivityTime( Math.abs( this.getTotalActivityTime()
                + Duration.between( Instant.now(),
                this.getLastActiveDate()
                        .toInstant() )
                .getSeconds() ) );

        CassandraDataControl
                .getInstance()
                .getAddTackerInfo()
                .apply( this.save( patrul, this.save( reqCar, position ) ) );
        return position; }

    public Position updateTime ( Position position, TupleOfCar tupleOfCar, Patrul patrul ) {
        this.setLastActiveDate( new Date() );
        this.setTotalActivityTime( Math.abs(
                this.getTotalActivityTime()
                        + Duration.between( Instant.now(),
                        this.getLastActiveDate().toInstant() ).getSeconds() ) );

        this.save( tupleOfCar, position );
        CassandraDataControlForEscort
                .getInstance()
                .getSaveTackerInfo()
                .apply( this.save( patrul, position ) );
        return position; }
}
