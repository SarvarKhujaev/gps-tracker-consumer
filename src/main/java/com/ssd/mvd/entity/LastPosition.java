package com.ssd.mvd.entity;

import com.ssd.mvd.inspectors.DataValidateInspector;
import com.ssd.mvd.database.CassandraDataControl;
import com.ssd.mvd.entity.patrulDataSet.Patrul;
import com.ssd.mvd.constants.Status;
import java.util.UUID;

public final class LastPosition extends DataValidateInspector {
    public String getIcon() {
        return this.icon;
    }

    public void setIcon( final String icon ) {
        this.icon = icon;
    }

    public String getIcon2() {
        return this.icon2;
    }

    public void setIcon2( final String icon2 ) {
        this.icon2 = icon2;
    }

    public String getCarType() {
        return this.carType;
    }

    public void setCarType( final String carType ) {
        this.carType = carType;
    }

    public String getTrackerId() {
        return this.trackerId;
    }

    public void setTrackerId( final String trackerId ) {
        this.trackerId = trackerId;
    }

    public String getCarGosNumber() {
        return this.carGosNumber;
    }

    public void setCarGosNumber( final String carGosNumber ) {
        this.carGosNumber = carGosNumber;
    }

    public Double getLastLatitude() {
        return this.lastLatitude;
    }

    public void setLastLatitude( final Double lastLatitude ) {
        this.lastLatitude = lastLatitude;
    }

    public Double getLastLongitude() {
        return this.lastLongitude;
    }

    public void setLastLongitude( final Double lastLongitude ) {
        this.lastLongitude = lastLongitude;
    }

    public Status getStatus() {
        return this.status;
    }

    public void setStatus( final Status status ) {
        this.status = status;
    }

    public UUID getPatrulUUID() {
        return this.patrulUUID;
    }

    public void setPatrulUUID( final UUID patrulUUID ) {
        this.patrulUUID = patrulUUID;
    }

    public String getTaskId() {
        return this.taskId;
    }

    public void setTaskId( final String taskId ) {
        this.taskId = taskId;
    }

    public String getPatrulName() {
        return this.patrulName;
    }

    public void setPatrulName( final String patrulName ) {
        this.patrulName = patrulName;
    }

    public String getPoliceType() {
        return this.policeType;
    }

    public void setPoliceType( final String policeType ) {
        this.policeType = policeType;
    }

    public String getPatrulpassportSeries() {
        return this.patrulpassportSeries;
    }

    public void setPatrulpassportSeries( final String patrulpassportSeries ) {
        this.patrulpassportSeries = patrulpassportSeries;
    }

    // Car data
    // иконка патрульного выбирается исходя из типа патрульного
    private String icon;
    // иконка патрульного выбирается исходя из типа патрульного
    private String icon2;
    private String carType;
    private String trackerId;
    private String carGosNumber;

    private Double lastLatitude;
    private Double lastLongitude;

    // Patrul data
    private Status status;
    private UUID patrulUUID;

    private String taskId;
    private String patrulName;
    private String policeType;
    private String patrulpassportSeries;

    public LastPosition ( final TrackerInfo trackerInfo ) {
        this.setCarType( trackerInfo.getReqCar().getVehicleType() );
        this.setTrackerId( trackerInfo.getReqCar().getTrackerId() );
        this.setLastLatitude( trackerInfo.getReqCar().getLatitude() );
        this.setCarGosNumber( trackerInfo.getReqCar().getGosNumber() );
        this.setLastLongitude( trackerInfo.getReqCar().getLongitude() );

        final Icons icons = super.icons.getOrDefault(
                trackerInfo.getPatrul().getPoliceType(),
                CassandraDataControl
                        .getInstance()
                        .getPoliceType
                        .apply( trackerInfo.getPatrul().getPoliceType() ) );

        this.setIcon( icons.getIcon1() );
        this.setIcon2( icons.getIcon2() );
        this.setPatrulUUID( trackerInfo.getPatrul().getUuid() );
        this.setPoliceType( trackerInfo.getPatrul().getPoliceType() );
        this.setStatus( trackerInfo.getPatrul().getPatrulTaskInfo().getStatus() );
        this.setTaskId( trackerInfo.getPatrul().getPatrulTaskInfo().getTaskId() );
        this.setPatrulName( trackerInfo.getPatrul().getPatrulFIOData().getName() );
        this.setPatrulpassportSeries( trackerInfo.getPatrul().getPassportNumber() );
    }

    public LastPosition ( final TrackerInfo trackerInfo, final Patrul patrul ) {
        this.setCarType( trackerInfo.getTupleOfCar().getCarModel() );
        this.setTrackerId( trackerInfo.getTupleOfCar().getTrackerId() );
        this.setLastLatitude( trackerInfo.getTupleOfCar().getLatitude() );
        this.setCarGosNumber( trackerInfo.getTupleOfCar().getGosNumber() );
        this.setLastLongitude( trackerInfo.getTupleOfCar().getLongitude() );

        if ( super.objectIsNotNull( patrul ) ) {
            this.setPatrulUUID( patrul.getUuid() );
            this.setPoliceType( patrul.getPoliceType() );
            this.setStatus( patrul.getPatrulTaskInfo().getStatus() );
            this.setTaskId( patrul.getPatrulTaskInfo().getTaskId() );
            this.setPatrulName( patrul.getPatrulFIOData().getName() );
            this.setPatrulpassportSeries( patrul.getPassportNumber() );
        }
    }
}
