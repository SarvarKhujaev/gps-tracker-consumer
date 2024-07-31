package com.ssd.mvd.entity;

import com.ssd.mvd.inspectors.EntitiesInstances;
import com.ssd.mvd.interfaces.EntityToCassandraConverter;
import com.ssd.mvd.inspectors.DataValidateInspector;
import com.ssd.mvd.database.CassandraDataControl;
import com.ssd.mvd.constants.CassandraFunctions;
import com.ssd.mvd.entity.patrulDataSet.Patrul;
import com.ssd.mvd.constants.CassandraCommands;
import com.ssd.mvd.constants.CassandraTables;
import com.ssd.mvd.inspectors.Inspector;

import com.datastax.driver.core.Row;

import java.text.MessageFormat;
import java.util.Date;

public final class TrackerInfo extends DataValidateInspector implements EntityToCassandraConverter {
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

    @Override
    public CassandraTables getEntityTableName () {
        return CassandraTables.TRACKERSID;
    }

    public TrackerInfo () {}

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
            final ReqCar reqCar
    ) {
        this.setStatus( true );

        final Icons icons = Inspector.icons.getOrDefault(
                patrul.getPoliceType(),
                new Icons().generate(
                        CassandraDataControl
                                .getInstance()
                                .getRowFromTabletsKeyspace(
                                        EntitiesInstances.ICONS,
                                        "policeType",
                                        patrul.getPoliceType()
                                )
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
        this.setLastActiveDate( super.newDate() );
        this.setDateOfRegistration( super.newDate() );
    }

    public TrackerInfo (
            final TupleOfCar tupleOfCar,
            final Row row
    ) {
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
            final TupleOfCar tupleOfCar
    ) {
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
            final Row row
    ) {
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
            final Row row
    ) {
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

    private void save (
            final Patrul patrul,
            final Position position
    ) {
        // обновляем позицию патрульного, и трекера
        position.update( patrul );

        final Icons icons = Inspector.icons.getOrDefault(
                patrul.getPoliceType(),
                new Icons().generate(
                        CassandraDataControl
                                .getInstance()
                                .getRowFromTabletsKeyspace(
                                        EntitiesInstances.POLICE_TYPE,
                                        "policeType",
                                        patrul.getPoliceType()
                                )
                )
        );

        position.update( icons );

        this.setPatrul( patrul );
        this.setIcon( icons.getIcon1() );
        this.setIcon2( icons.getIcon2() );
        this.setPatrulPassportSeries( this.getPatrul().getPassportNumber() );
    }

    private Position save (
            final ReqCar reqCar,
            final Position position
    ) {
        position.setCarGosNumber( reqCar.getGosNumber() );
        position.setCarType( reqCar.getVehicleType() );

        reqCar.setLongitude( position.getLongitude() );
        reqCar.setLatitude( position.getLatitude() );

        this.setLongitude( position.getLongitude() );
        this.setLatitude( position.getLatitude() );
        this.setGosNumber( reqCar.getGosNumber() );
        this.setReqCar( reqCar );

        if ( super.check( position ) ) {
            this.getReqCar().updateEntity();
        }

        return position;
    }

    private void save (
            final TupleOfCar tupleOfCar,
            final Position position
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
            final Position position,
            final TupleOfCar tupleOfCar
    ) {
        this.setPatrul( null );
        this.setPatrulPassportSeries( null );
        this.setLastActiveDate( super.newDate() );
        this.setTotalActivityTime(
                super.getTimeDifference( this.getTotalActivityTime(), this.getLastActiveDate().toInstant() )
        );

        this.save( tupleOfCar, position );

        return position;
    }

    public Position updateTime (
            final Position position,
            final ReqCar reqCar,
            final Patrul patrul
    ) {
        this.setSpeed( position.getSpeed() );
        this.updateEntity();

        this.setLastActiveDate( super.newDate() );
        this.setTotalActivityTime(
                super.getTimeDifference( this.getTotalActivityTime(), this.getLastActiveDate().toInstant() )
        );

        this.save( patrul, this.save( reqCar, position ) );
        return position;
    }

    public Position updateTime (
            final Position position,
            final TupleOfCar tupleOfCar,
            final Patrul patrul
    ) {
        this.setLastActiveDate( super.newDate() );
        this.setTotalActivityTime(
                super.getTimeDifference( this.getTotalActivityTime(), this.getLastActiveDate().toInstant() )
        );

        this.save( tupleOfCar, position );
        this.save( patrul, position );
        return position;
    }

    @Override
    public String getEntityUpdateCommand() {
        return MessageFormat.format(
                """
                {0} {1}.{2}
                ( imei, date, speed, distance )
                VALUES( {3}, {4}, {5}, {6} );
                """,
                CassandraCommands.INSERT_INTO,

                this.getEntityKeyspaceName(),
                CassandraTables.TRACKER_FUEL_CONSUMPTION,

                super.joinWithAstrix( this.getTrackerId() ),
                CassandraFunctions.TO_TIMESTAMP.formatted( CassandraFunctions.NOW ),
                this.getSpeed(),
                ( ( this.getSpeed() * 10 / 36 ) * 15 )
        );
    }

    @Override
    public String getEntityInsertCommand() {
        return MessageFormat.format(
                """
                {0} {1}.{2}
                (
                    trackersId,
                    patrulPassportSeries,
                    gosnumber,
                    policeType,
                    policeType2,
                    status,
                    latitude,
                    longitude,
                    totalActivityTime,
                    lastActiveDate,
                    dateOfRegistration
                )
                VALUES( {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11,number,#}, {12}, {13} );
                """,
                CassandraCommands.INSERT_INTO,

                this.getEntityKeyspaceName(),
                CassandraTables.TRACKERSID,

                super.joinWithAstrix( this.getTrackerId() ),
                super.joinWithAstrix( this.getPatrulPassportSeries() ),
                super.joinWithAstrix( this.getGosNumber() ),
                super.joinWithAstrix( this.getIcon() ),
                super.joinWithAstrix( this.getIcon2() ),
                super.joinWithAstrix( this.getStatus() ),

                this.getLatitude(),
                this.getLongitude(),
                this.getTotalActivityTime(),

                CassandraFunctions.TO_TIMESTAMP.formatted( CassandraFunctions.NOW ),
                super.joinWithAstrix( this.getDateOfRegistration() )
        );
    }

    @Override
    public String getEntityDeleteCommand() {
        return MessageFormat.format(
                """
                {0} {1}.{2}
                (
                    trackersId,
                    patrulPassportSeries,
                    gosnumber,
                    status,
                    latitude,
                    longitude,
                    totalActivityTime,
                    lastActiveDate,
                    dateOfRegistration
                )
                VALUES( {3}, {4}, {5}, {6}, {7}, {8}, {9,number,#}, {10}, {11} );
                """,
                CassandraCommands.INSERT_INTO,

                CassandraTables.ESCORT,
                CassandraTables.TRACKERSID,

                super.joinWithAstrix( this.getTrackerId() ),
                super.joinWithAstrix( this.getPatrulPassportSeries() ),
                super.joinWithAstrix( this.getGosNumber() ),
                super.joinWithAstrix( this.getStatus() ),

                this.getLatitude(),
                this.getLongitude(),
                this.getTotalActivityTime(),

                CassandraFunctions.TO_TIMESTAMP.formatted( CassandraFunctions.NOW ),
                super.joinWithAstrix( this.getDateOfRegistration() )
        );
    }
}
