package com.ssd.mvd.entity;

import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.core.CqlIdentifier;

import com.datastax.driver.core.GettableData;
import com.datastax.driver.core.Row;

import com.ssd.mvd.inspectors.dataTypesInpectors.TimeInspector;
import com.ssd.mvd.inspectors.*;

import com.ssd.mvd.interfaces.entity.EntityToCassandraConverter;
import com.ssd.mvd.annotations.entity.object.EntityAnnotations;
import com.ssd.mvd.database.CassandraDataControl;
import com.ssd.mvd.entity.patrulDataSet.Patrul;

import com.ssd.mvd.constants.cassandra.CassandraTables;

import java.lang.ref.WeakReference;
import java.util.Date;

@EntityAnnotations(
        name = "TrackerInfo",
        tableName = CassandraTables.TRACKERSID,
        keysapceName = CassandraTables.ESCORT,
        primaryKeys = { "trackersId" }
)
public final class TrackerInfo implements EntityToCassandraConverter {
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

    public double getLatitude() {
        return this.latitude;
    }

    public void setLatitude ( final double latitude ) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return this.longitude;
    }

    public void setLongitude ( final double longitude ) {
        this.longitude = longitude;
    }

    public boolean getStatus() {
        return this.status;
    }

    public void setStatus ( final boolean status ) {
        this.status = status;
    }

    public long getTotalActivityTime() {
        return this.totalActivityTime;
    }

    public void setTotalActivityTime ( final long totalActivityTime ) {
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

    public double getSpeed() {
        return this.speed;
    }

    public void setSpeed( final double speed ) {
        this.speed = speed;
    }

    private ReqCar reqCar;
    private Patrul patrul;
    private TupleOfCar tupleOfCar;

    private String icon;
    private String icon2;
    private String trackerId;
    private String gosNumber;
    private String patrulPassportSeries;

    private double speed;
    private double latitude;
    private double longitude;

    private boolean status;
    private long totalActivityTime;

    private Date lastActiveDate;
    private Date dateOfRegistration;

    public TrackerInfo () {}

    public TrackerInfo ( final TupleOfCar tupleOfCar ) {
        this.setStatus( true );

        this.setLastActiveDate( TimeInspector.newDate() );
        this.setDateOfRegistration( TimeInspector.newDate() );

        this.setTupleOfCar( tupleOfCar );
        this.setGosNumber( tupleOfCar.getGosNumber() );
        this.setTrackerId( tupleOfCar.getTrackerId() );
    }

    public TrackerInfo (
            final Patrul patrul,
            final ReqCar reqCar
    ) {
        this.setStatus( true );

        final Icons icons = Inspector.icons.getOrDefault(
                patrul.getPoliceType(),
                EntitiesInstances.ICONS.get().generate().generate(
                        CassandraDataControl
                                .getInstance()
                                .getRowFromTabletsKeyspace(
                                        EntitiesInstances.ICONS.get(),
                                        "policeType",
                                        patrul.getPoliceType()
                                ).get()
                )
        );

        this.setIcon( icons.getIcon1() );
        this.setIcon2( icons.getIcon2() );

        this.setPatrul( patrul );
        this.setPatrulPassportSeries( patrul.getPassportNumber() );

        this.setReqCar( reqCar );
        this.setGosNumber( reqCar.getGosNumber() );
        this.setTrackerId( reqCar.getTrackerId() );

        this.setTotalActivityTime( 0L );
        this.setLastActiveDate( TimeInspector.newDate() );
        this.setDateOfRegistration( TimeInspector.newDate() );
    }

    public TrackerInfo (
            @lombok.NonNull final WeakReference< TupleOfCar > tupleOfCar,
            @lombok.NonNull final GettableData row
    ) {
        this.setTupleOfCar( tupleOfCar.get() );
        this.setGosNumber( tupleOfCar.get().getGosNumber() );

        this.setStatus( row.getBool( "status" ) );
        this.setLatitude( row.getDouble( "latitude" ) );
        this.setLongitude( row.getDouble( "longitude" ) );
        this.setTrackerId( row.getString( "trackersId" ) );
        this.setLastActiveDate( row.getTimestamp( "lastActiveDate" ) );
        this.setDateOfRegistration( row.getTimestamp( "dateofregistration" ) );
        this.setTotalActivityTime( Math.abs( (long) row.getDouble( "totalActivityTime" ) ) );

        CustomServiceCleaner.clearReference( tupleOfCar );
    }

    public TrackerInfo (
            @lombok.NonNull final WeakReference< Patrul > patrul,
            @lombok.NonNull final TupleOfCar tupleOfCar
    ) {
        this.setStatus( true );
        this.setReqCar( null );
        this.setTotalActivityTime( 0L );

        this.setTupleOfCar( tupleOfCar );
        this.setGosNumber( tupleOfCar.getGosNumber() );
        this.setTrackerId( tupleOfCar.getTrackerId() );

        this.setLastActiveDate( TimeInspector.newDate() );
        this.setDateOfRegistration( TimeInspector.newDate() );

        this.setPatrul( patrul.get() );
        this.setPatrulPassportSeries( patrul.get().getPassportNumber() );

        CustomServiceCleaner.clearReference( patrul );
    }

    public TrackerInfo (
            @lombok.NonNull final Patrul patrul,
            @lombok.NonNull final ReqCar reqCar,
            @lombok.NonNull final Row row
    ) {
        this.setPatrul( patrul );
        this.setPatrulPassportSeries( patrul.getPassportNumber() );

        this.setReqCar( reqCar );
        this.setGosNumber( reqCar.getGosNumber() );

        this.setIcon( row.getString( "policeType" ) );
        this.setIcon2( row.getString( "policeType2" ) );
        this.setStatus( row.getBool( "status" ) );
        this.setLatitude( row.getDouble( "latitude" ) );
        this.setLongitude( row.getDouble( "longitude" ) );
        this.setTrackerId( row.getString( "trackersId" ) );
        this.setLastActiveDate( row.getTimestamp( "lastActiveDate" ) );
        this.setTotalActivityTime( Math.abs( (long) row.getDouble( "totalActivityTime" ) ) );
        this.setDateOfRegistration( row.getTimestamp( "dateofregistration" ) );
    }

    public TrackerInfo (
            @lombok.NonNull final WeakReference< Patrul > patrul,
            @lombok.NonNull final WeakReference< TupleOfCar > tupleOfCar,
            @lombok.NonNull final GettableData row
    ) {
        this.setPatrul( patrul.get() );

        this.setTupleOfCar( tupleOfCar.get() );
        this.setGosNumber( tupleOfCar.get().getGosNumber() );

        this.setStatus( row.getBool( "status" ) );
        this.setLatitude( row.getDouble( "latitude" ) );
        this.setLongitude( row.getDouble( "longitude" ) );
        this.setTrackerId( row.getString( "trackersId" ) );
        this.setLastActiveDate( row.getTimestamp( "lastActiveDate" ) );
        this.setTotalActivityTime( Math.abs( (long) row.getDouble( "totalActivityTime" ) ) );
        this.setDateOfRegistration( row.getTimestamp( "dateofregistration" ) );
        this.setPatrulPassportSeries( patrul.get().getPassportNumber() );

        CustomServiceCleaner.clearReference( patrul );
        CustomServiceCleaner.clearReference( tupleOfCar );
    }

    private void save (
            @lombok.NonNull final Patrul patrul,
            @lombok.NonNull final Position position
    ) {
        // обновляем позицию патрульного, и трекера
        position.update( patrul );

        final Icons icons = Inspector.icons.getOrDefault(
                patrul.getPoliceType(),
                EntitiesInstances.ICONS.get().generate().generate(
                        CassandraDataControl
                                .getInstance()
                                .getRowFromTabletsKeyspace(
                                        EntitiesInstances.POLICE_TYPE.get(),
                                        "policeType",
                                        patrul.getPoliceType()
                                ).get()
                )
        );

        position.update( icons );

        this.setPatrul( patrul );
        this.setIcon( icons.getIcon1() );
        this.setIcon2( icons.getIcon2() );
        this.setPatrulPassportSeries( this.getPatrul().getPassportNumber() );
    }

    private Position save (
            @lombok.NonNull final ReqCar reqCar,
            @lombok.NonNull final Position position
    ) {
        position.setCarGosNumber( reqCar.getGosNumber() );
        position.setCarType( reqCar.getVehicleType() );

        reqCar.setLongitude( position.getLongitude() );
        reqCar.setLatitude( position.getLatitude() );

        this.setLongitude( position.getLongitude() );
        this.setLatitude( position.getLatitude() );
        this.setGosNumber( reqCar.getGosNumber() );
        this.setReqCar( reqCar );

        if ( DataValidateInspector.check( position ) ) {
            this.getReqCar().updateEntity();
        }

        return position;
    }

    private void save (
            @lombok.NonNull final TupleOfCar tupleOfCar,
            @lombok.NonNull final Position position
    ) {
        // обновляем позицию патрульного, и трекера
        position.setCarGosNumber( tupleOfCar.getGosNumber() );
        position.setCarType( tupleOfCar.getCarModel() );

        tupleOfCar.setLongitude( position.getLongitude() );
        tupleOfCar.setLatitude( position.getLatitude() );

        this.setGosNumber( tupleOfCar.getGosNumber() );
        this.setTupleOfCar( tupleOfCar );

        this.setLongitude( position.getLongitude() );
        this.setLatitude( position.getLatitude() );

        tupleOfCar.updateEntity();
    }

    public Position updateTime (
            @lombok.NonNull final Position position,
            @lombok.NonNull final TupleOfCar tupleOfCar
    ) {
        this.setPatrul( null );
        this.setPatrulPassportSeries( null );
        this.setLastActiveDate( TimeInspector.newDate() );
        this.setTotalActivityTime(
                TimeInspector.getTimeDifference( this.getTotalActivityTime(), this.getLastActiveDate().toInstant() )
        );

        this.save( tupleOfCar, position );

        return position;
    }

    public Position updateTime (
            @lombok.NonNull final Position position,
            @lombok.NonNull final ReqCar reqCar,
            @lombok.NonNull final Patrul patrul
    ) {
        this.setSpeed( position.getSpeed() );
        this.updateEntity();

        this.setLastActiveDate( TimeInspector.newDate() );
        this.setTotalActivityTime(
                TimeInspector.getTimeDifference( this.getTotalActivityTime(), this.getLastActiveDate().toInstant() )
        );

        this.save( patrul, this.save( reqCar, position ) );

        return position;
    }

    public Position updateTime (
            @lombok.NonNull final Position position,
            @lombok.NonNull final TupleOfCar tupleOfCar,
            @lombok.NonNull final Patrul patrul
    ) {
        this.setLastActiveDate( TimeInspector.newDate() );
        this.setTotalActivityTime(
                TimeInspector.getTimeDifference( this.getTotalActivityTime(), this.getLastActiveDate().toInstant() )
        );

        this.save( tupleOfCar, position );
        this.save( patrul, position );

        return position;
    }

    @Override
    @lombok.NonNull
    public Insert getEntityInsert() {
        return this.startInsert()
                .value(
                        CqlIdentifier.fromCql( "trackersId" ),
                        QueryBuilder.literal( this.getTrackerId() )
                ).value(
                        CqlIdentifier.fromCql( "patrulPassportSeries" ),
                        QueryBuilder.literal( this.getPatrulPassportSeries() )
                ).value(
                        CqlIdentifier.fromCql( "gosnumber" ),
                        QueryBuilder.literal( this.getGosNumber() )
                ).value(
                        CqlIdentifier.fromCql( "policeType" ),
                        QueryBuilder.literal( this.getIcon() )
                ).value(
                        CqlIdentifier.fromCql( "policeType2" ),
                        QueryBuilder.literal( this.getIcon2() )
                ).value(
                        CqlIdentifier.fromCql( "status" ),
                        QueryBuilder.literal( this.getStatus() )
                ).value(
                        CqlIdentifier.fromCql( "latitude" ),
                        QueryBuilder.literal( this.getLatitude() )
                ).value(
                        CqlIdentifier.fromCql( "longitude" ),
                        QueryBuilder.literal( this.getLongitude() )
                ).value(
                        CqlIdentifier.fromCql( "totalActivityTime" ),
                        QueryBuilder.literal( this.getTotalActivityTime() )
                ).value(
                        CqlIdentifier.fromCql( "lastActiveDate" ),
                        QueryBuilder.now()
                ).value(
                        CqlIdentifier.fromCql( "dateOfRegistration" ),
                        QueryBuilder.literal( this.getDateOfRegistration() )
                );
    }

    @lombok.NonNull
    @org.jetbrains.annotations.Contract( value = "_ -> !null" )
    public Select getEntitySelect (
            final Object ... params
    ) {
        return this.startSelect()
                .column( "lastActiveDate" )
                .where(
                        Relation.column(
                                CqlIdentifier.fromCql( "trackersId" )
                        ).isEqualTo( QueryBuilder.literal( params[0] ) )
                );
    }
}
