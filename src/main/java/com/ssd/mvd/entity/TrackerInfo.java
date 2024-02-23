package com.ssd.mvd.entity;

import com.ssd.mvd.database.CassandraDataControlForEscort;
import com.ssd.mvd.inspectors.DataValidateInspector;
import com.ssd.mvd.database.CassandraDataControl;
import com.ssd.mvd.entity.patrulDataSet.Patrul;

import com.datastax.driver.core.Row;
import java.util.Date;

public final class TrackerInfo extends DataValidateInspector {
    public ReqCar getReqCar() {
        return this.reqCar;
    }

    public void setReqCar ( final ReqCar reqCar ) {
        this.reqCar = reqCar;
    }

    public Patrul getPatrul() {
        return this.patrul;
    }

    public void setPatrul ( final Patrul patrul ) {
        this.patrul = patrul;
    }

    public TupleOfCar getTupleOfCar() {
        return this.tupleOfCar;
    }

    public void setTupleOfCar ( final TupleOfCar tupleOfCar ) {
        this.tupleOfCar = tupleOfCar;
    }

    public String getIcon() {
        return this.icon;
    }

    public void setIcon ( final String icon ) {
        this.icon = icon;
    }

    public String getIcon2() {
        return this.icon2;
    }

    public void setIcon2 ( final String icon2 ) {
        this.icon2 = icon2;
    }

    public String getTrackerId() {
        return this.trackerId;
    }

    public void setTrackerId ( final String trackerId ) {
        this.trackerId = trackerId;
    }

    public String getGosNumber() {
        return this.gosNumber;
    }

    public void setGosNumber ( final String gosNumber ) {
        this.gosNumber = gosNumber;
    }

    public String getPatrulPassportSeries() {
        return this.patrulPassportSeries;
    }

    public void setPatrulPassportSeries ( final String patrulPassportSeries ) {
        this.patrulPassportSeries = patrulPassportSeries;
    }

    public Double getLatitude() {
        return this.latitude;
    }

    public void setLatitude ( final Double latitude ) {
        this.latitude = latitude;
    }

    public Double getLongitude() {
        return this.longitude;
    }

    public void setLongitude ( final Double longitude ) {
        this.longitude = longitude;
    }

    public Boolean getStatus() {
        return this.status;
    }

    public void setStatus ( final Boolean status ) {
        this.status = status;
    }

    public Long getTotalActivityTime() {
        return this.totalActivityTime;
    }

    public void setTotalActivityTime ( final Long totalActivityTime ) {
        this.totalActivityTime = totalActivityTime;
    }

    public Date getLastActiveDate() {
        return this.lastActiveDate;
    }

    public void setLastActiveDate ( final Date lastActiveDate ) {
        this.lastActiveDate = lastActiveDate;
    }

    public Date getDateOfRegistration() {
        return this.dateOfRegistration;
    }

    public void setDateOfRegistration ( final Date dateOfRegistration ) {
        this.dateOfRegistration = dateOfRegistration;
    }

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
    private Date dateOfRegistration;

    public TrackerInfo ( final TupleOfCar tupleOfCar ) {
        this.setStatus( true );

        this.setLastActiveDate( super.newDate() );
        this.setDateOfRegistration( super.newDate() );

        this.setTupleOfCar( tupleOfCar );
        this.setGosNumber( tupleOfCar.getGosNumber() );
        this.setTrackerId( tupleOfCar.getTrackerId() );
    }

    public TrackerInfo (
            final Patrul patrul,
            final ReqCar reqCar ) {
        this.setStatus( true );

        final Icons icons = super.icons.getOrDefault(
                patrul.getPoliceType(),
                CassandraDataControl
                        .getInstance()
                        .getPoliceType
                        .apply( patrul.getPoliceType() ) );

        this.setIcon( icons.getIcon1() );
        this.setIcon2( icons.getIcon2() );

        this.setPatrul( patrul );
        this.setPatrulPassportSeries( patrul.getPassportNumber() );

        this.setReqCar( reqCar );
        this.setGosNumber( reqCar.getGosNumber() );
        this.setTrackerId( reqCar.getTrackerId() );

        this.setTotalActivityTime( 0L );
        this.setLastActiveDate( super.newDate() );
        this.setDateOfRegistration( super.newDate() );
    }

    public TrackerInfo (
            final TupleOfCar tupleOfCar,
            final Row row ) {
        this.setTupleOfCar( tupleOfCar );
        this.setGosNumber( tupleOfCar.getGosNumber() );

        this.setStatus( row.getBool( "status" ) );
        this.setLatitude( row.getDouble( "latitude" ) );
        this.setLongitude( row.getDouble( "longitude" ) );
        this.setTrackerId( row.getString( "trackersId" ) );
        this.setLastActiveDate( row.getTimestamp( "lastActiveDate" ) );
        this.setDateOfRegistration( row.getTimestamp( "dateofregistration" ) );
        this.setTotalActivityTime( Math.abs( (long) row.getDouble( "totalActivityTime" ) ) );
    }

    public TrackerInfo (
            final Patrul patrul,
            final TupleOfCar tupleOfCar ) {
        this.setStatus( true );
        this.setReqCar( null );
        this.setTotalActivityTime( 0L );

        this.setTupleOfCar( tupleOfCar );
        this.setGosNumber( tupleOfCar.getGosNumber() );
        this.setTrackerId( tupleOfCar.getTrackerId() );

        this.setLastActiveDate( super.newDate() );
        this.setDateOfRegistration( super.newDate() );

        this.setPatrul( patrul );
        this.setPatrulPassportSeries( patrul.getPassportNumber() );
    }

    public TrackerInfo (
            final Patrul patrul,
            final ReqCar reqCar,
            final Row row ) {
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
        this.setTotalActivityTime( Math.abs( (long) row.getDouble( "totalActivityTime" ) ) );
    }

    public TrackerInfo (
            final Patrul patrul,
            final TupleOfCar tupleOfCar,
            final Row row ) {
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
        this.setTotalActivityTime( Math.abs( (long) row.getDouble( "totalActivityTime" ) ) );
    }

    private void save ( final Patrul patrul,  final Position position ) {
        // обновляем позицию патрульного, и трекера
        position.setLongitudeOfTask( patrul.getPatrulLocationData().getLongitudeOfTask() );
        position.setLatitudeOfTask( patrul.getPatrulLocationData().getLatitudeOfTask() );
        position.setPatrulName( patrul.getPatrulFIOData().getName() );
        position.setTaskId( patrul.getPatrulTaskInfo().getTaskId() );
        position.setStatus( patrul.getPatrulTaskInfo().getStatus() );
        position.setPoliceType( patrul.getPoliceType() );
        position.setPatrulUUID( patrul.getUuid() );

        final Icons icons = super.icons.getOrDefault(
                patrul.getPoliceType(),
                CassandraDataControl
                        .getInstance()
                        .getPoliceType
                        .apply( patrul.getPoliceType() ) );

        position.setIcon( icons.getIcon1() );
        position.setIcon2( icons.getIcon2() );

        position.setRegionId( patrul.getPatrulRegionData().getRegionId() );
        position.setMahallaId( patrul.getPatrulRegionData().getMahallaId() );
        position.setDistrictId( patrul.getPatrulRegionData().getDistrictId() );

        this.setPatrul( patrul );
        this.setIcon( icons.getIcon1() );
        this.setIcon2( icons.getIcon2() );
        this.setPatrulPassportSeries( this.getPatrul().getPassportNumber() );
    }

    private Position save ( final ReqCar reqCar, final Position position ) {
        position.setCarGosNumber( reqCar.getGosNumber() );
        position.setCarType( reqCar.getVehicleType() );

        reqCar.setLongitude( position.getLongitude() );
        reqCar.setLatitude( position.getLatitude() );

        this.setLongitude( position.getLongitude() );
        this.setLatitude( position.getLatitude() );
        this.setGosNumber( reqCar.getGosNumber() );
        this.setReqCar( reqCar );

        if ( super.check( position ) ) {
            CassandraDataControl
                    .getInstance()
                    .getUpdateReqCarPosition()
                    .accept( this.getReqCar() );
        }

        return position;
    }

    private void save ( final TupleOfCar tupleOfCar,  final Position position ) {
        // обновляем позицию патрульного, и трекера
        position.setCarGosNumber( tupleOfCar.getGosNumber() );
        position.setCarType( tupleOfCar.getCarModel() );

        tupleOfCar.setLongitude( position.getLongitude() );
        tupleOfCar.setLatitude( position.getLatitude() );

        this.setGosNumber( tupleOfCar.getGosNumber() );
        this.setTupleOfCar( tupleOfCar );

        this.setLongitude( position.getLongitude() );
        this.setLatitude( position.getLatitude() );

        CassandraDataControlForEscort
                .getInstance()
                .updateEscortCarLocation(
                        tupleOfCar.getLongitude(),
                        tupleOfCar.getLatitude(),
                        tupleOfCar );
    }

    public Position updateTime ( final Position position, final TupleOfCar tupleOfCar ) {
        this.setPatrul( null );
        this.setPatrulPassportSeries( null );
        this.setLastActiveDate( super.newDate() );
        this.setTotalActivityTime(
                super.getTimeDifference( this.getTotalActivityTime(), this.getLastActiveDate().toInstant() ) );

        this.save( tupleOfCar, position );

        return position;
    }

    public Position updateTime (
            final Position position,
            final ReqCar reqCar,
            final Patrul patrul ) {
        CassandraDataControl
                .getInstance()
                .getSaveFuelConsumptionOfCar()
                .accept( this, position.getSpeed() );

        this.setLastActiveDate( super.newDate() );
        this.setTotalActivityTime(
                super.getTimeDifference( this.getTotalActivityTime(), this.getLastActiveDate().toInstant() ) );

        this.save( patrul, this.save( reqCar, position ) );
        return position;
    }

    public Position updateTime (
            final Position position,
            final TupleOfCar tupleOfCar,
            final Patrul patrul ) {
        this.setLastActiveDate( super.newDate() );
        this.setTotalActivityTime(
                super.getTimeDifference( this.getTotalActivityTime(), this.getLastActiveDate().toInstant() ) );

        this.save( tupleOfCar, position );
        this.save( patrul, position );
        return position;
    }
}
