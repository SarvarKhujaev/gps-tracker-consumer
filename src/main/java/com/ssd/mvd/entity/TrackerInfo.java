package com.ssd.mvd.entity;

import com.ssd.mvd.database.CassandraDataControlForEscort;
import com.ssd.mvd.inspectors.DataValidateInspector;
import com.ssd.mvd.database.CassandraDataControl;
import com.datastax.driver.core.Row;
import java.util.Date;

@lombok.Data
@lombok.NoArgsConstructor
@lombok.AllArgsConstructor
public final class TrackerInfo {
    private ReqCar reqCar;
    private Patrul patrul;
    private TupleOfCar tupleOfCar;

    private String icon;
    private String icon2;
    private String trackerId;
    private String gosNumber;
    private String patrulPassportSeries;

    private Double latitude;
    private Double longitude;

    private Boolean status;
    private Long totalActivityTime;

    private Date lastActiveDate;
    private Date dateOfRegistration = new Date();

    public TrackerInfo ( final TupleOfCar tupleOfCar ) {
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

    public TrackerInfo ( final Patrul patrul,  final ReqCar reqCar ) {
        this.setStatus( true );
        this.setTupleOfCar( null );

        final Icons icons = CassandraDataControl
                .getInstance()
                .icons
                .getOrDefault( patrul.getPoliceType(),
                        CassandraDataControl
                                .getInstance()
                                .getPoliceType
                                .apply( patrul.getPoliceType() ) );

        this.setIcon( icons.getIcon1() );
        this.setIcon2( icons.getIcon2() );

        this.setPatrul( patrul );
        this.setReqCar( reqCar );
        this.setGosNumber( reqCar.getGosNumber() );

        this.setTotalActivityTime( 0L );
        this.setLastActiveDate( new Date() );
        this.setDateOfRegistration( new Date() );
        this.setTrackerId( reqCar.getTrackerId() );
        this.setPatrulPassportSeries( patrul.getPassportNumber() ); }

    public TrackerInfo ( final TupleOfCar tupleOfCar,  final Row row ) {
        this.setTupleOfCar( tupleOfCar );
        this.setStatus( row.getBool( "status" ) );
        this.setGosNumber( tupleOfCar.getGosNumber() );
        this.setLatitude( row.getDouble( "latitude" ) );
        this.setLongitude( row.getDouble( "longitude" ) );
        this.setTrackerId( row.getString( "trackersId" ) );
        this.setLastActiveDate( row.getTimestamp( "lastActiveDate" ) );
        this.setDateOfRegistration( row.getTimestamp( "dateofregistration" ) );
        this.setTotalActivityTime( Math.abs( (long) row.getDouble( "totalActivityTime" ) ) ); }

    public TrackerInfo ( final Patrul patrul,  final TupleOfCar tupleOfCar ) {
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

    public TrackerInfo ( final Patrul patrul,  final ReqCar reqCar, final Row row ) {
        this.setPatrul( patrul );
        this.setReqCar( reqCar );
        this.setGosNumber( reqCar.getGosNumber() );
        this.setStatus( row.getBool( "status" ) );
        this.setIcon( row.getString( "policeType" ) );
        this.setIcon2( row.getString( "policeType2" ) );
        this.setLatitude( row.getDouble( "latitude" ) );
        this.setLongitude( row.getDouble( "longitude" ) );
        this.setTrackerId( row.getString( "trackersId" ) );
        this.setPatrulPassportSeries( patrul.getPassportNumber() );
        this.setLastActiveDate( row.getTimestamp( "lastActiveDate" ) );
        this.setDateOfRegistration( row.getTimestamp( "dateofregistration" ) );
        this.setTotalActivityTime( Math.abs( (long) row.getDouble( "totalActivityTime" ) ) ); }

    public TrackerInfo ( final Patrul patrul,  final TupleOfCar tupleOfCar, final Row row ) {
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

    private Position save ( final ReqCar reqCar, final Position position ) {
        position.setCarGosNumber( reqCar.getGosNumber() );
        position.setCarType( reqCar.getVehicleType() );

        reqCar.setLongitude( position.getLongitude() );
        reqCar.setLatitude( position.getLatitude() );

        this.setLongitude( position.getLongitude() );
        this.setLatitude( position.getLatitude() );
        this.setGosNumber( reqCar.getGosNumber() );
        this.setReqCar( reqCar );

        if ( DataValidateInspector
                .getInstance()
                .check
                .test( position, 6 ) ) CassandraDataControl
                .getInstance()
                .getUpdateReqCarPosition()
                .accept( this.getReqCar() );

        return position; }

    private TrackerInfo save ( final Patrul patrul,  final Position position ) {
        // обновляем позицию патрульного, и трекера
        position.setLongitudeOfTask( patrul.getLongitudeOfTask() );
        position.setLatitudeOfTask( patrul.getLatitudeOfTask() );
        position.setPoliceType( patrul.getPoliceType() );
        position.setPatrulUUID( patrul.getUuid() );
        position.setPatrulName( patrul.getName() );
        position.setStatus( patrul.getStatus() );
        position.setTaskId( patrul.getTaskId() );

        final Icons icons = CassandraDataControl
                .getInstance()
                .icons
                .getOrDefault( patrul.getPoliceType(),
                        CassandraDataControl
                                .getInstance()
                                .getPoliceType
                                .apply( patrul.getPoliceType() ) );

        position.setIcon( icons.getIcon1() );
        position.setIcon2( icons.getIcon2() );

        position.setRegionId( patrul.getRegionId() );
        position.setMahallaId( patrul.getMahallaId() );
        position.setDistrictId( patrul.getDistrictId() );

        this.setPatrul( patrul );
        this.setIcon( icons.getIcon1() );
        this.setIcon2( icons.getIcon2() );
        this.setPatrulPassportSeries( this.getPatrul().getPassportNumber() );
        return this; }

    private TrackerInfo save ( final TupleOfCar tupleOfCar,  final Position position ) {
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
                .updateEscortCar(
                        tupleOfCar.getLongitude(),
                        tupleOfCar.getLatitude(),
                        tupleOfCar );
        return this; }

    public Position updateTime ( final Position position, final TupleOfCar tupleOfCar ) {
        this.setPatrul( null );
        this.setPatrulPassportSeries( null );
        this.setLastActiveDate( new Date() );
        this.setTotalActivityTime( CassandraDataControl
                .getInstance()
                .getTimeDifference
                .apply( this.getTotalActivityTime(), this.getLastActiveDate().toInstant() ) );

        CassandraDataControlForEscort
                .getInstance()
                .getSaveTackerInfo()
                .apply( this.save( tupleOfCar, position ) );

        return position; }

    public Position updateTime ( final Position position, final ReqCar reqCar, final Patrul patrul ) {
        CassandraDataControl
                .getInstance()
                .getAddValue()
                .accept( this, position.getSpeed() );

        this.setLastActiveDate( new Date() );
        this.setTotalActivityTime( CassandraDataControl
                .getInstance()
                .getTimeDifference
                .apply( this.getTotalActivityTime(), this.getLastActiveDate().toInstant() ) );

        CassandraDataControl
                .getInstance()
                .getAddTackerInfo()
                .apply( this.save( patrul, this.save( reqCar, position ) ) );

        return position; }

    public Position updateTime ( final Position position, final TupleOfCar tupleOfCar, final Patrul patrul ) {
        this.setLastActiveDate( new Date() );
        this.setTotalActivityTime( CassandraDataControl
                .getInstance()
                .getTimeDifference
                .apply( this.getTotalActivityTime(), this.getLastActiveDate().toInstant() ) );

        this.save( tupleOfCar, position );

        CassandraDataControlForEscort
                .getInstance()
                .getSaveTackerInfo()
                .apply( this.save( patrul, position ) );

        return position; }
}
