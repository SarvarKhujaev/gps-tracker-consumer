package com.ssd.mvd.entity;

import com.ssd.mvd.constants.Status;
import com.ssd.mvd.database.CassandraDataControl;
import com.ssd.mvd.entity.patrulDataSet.Patrul;
import com.ssd.mvd.inspectors.DataValidateInspector;
import com.ssd.mvd.inspectors.EntitiesInstances;
import com.ssd.mvd.inspectors.Inspector;

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

    public double getLastLatitude() {
        return this.lastLatitude;
    }

    public void setLastLatitude( final double lastLatitude ) {
        this.lastLatitude = lastLatitude;
    }

    public double getLastLongitude() {
        return this.lastLongitude;
    }

    public void setLastLongitude( final double lastLongitude ) {
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

    private double lastLatitude;
    private double lastLongitude;

    // Patrul data
    private Status status;
    private UUID patrulUUID;

    private String taskId;
    private String patrulName;
    private String policeType;
    private String patrulpassportSeries;

    public LastPosition ( final TrackerInfo trackerInfo ) {
        super.checkAndSetParams(
                trackerInfo,
                trackerInfo1 -> {
                    this.setCarType( trackerInfo1.getReqCar().getVehicleType() );
                    this.setTrackerId( trackerInfo1.getReqCar().getTrackerId() );
                    this.setLastLatitude( trackerInfo1.getReqCar().getLatitude() );
                    this.setCarGosNumber( trackerInfo1.getReqCar().getGosNumber() );
                    this.setLastLongitude( trackerInfo1.getReqCar().getLongitude() );

                    this.setPatrulUUID( trackerInfo1.getPatrul().getUuid() );
                    this.setPoliceType( trackerInfo1.getPatrul().getPoliceType() );
                    this.setStatus( trackerInfo1.getPatrul().getPatrulTaskInfo().getStatus() );
                    this.setTaskId( trackerInfo1.getPatrul().getPatrulTaskInfo().getTaskId() );
                    this.setPatrulName( trackerInfo1.getPatrul().getPatrulFIOData().getName() );
                    this.setPatrulpassportSeries( trackerInfo1.getPatrul().getPassportNumber() );
                }
        );

        super.checkAndSetParams(
                Inspector.icons.getOrDefault(
                        trackerInfo.getPatrul().getPoliceType(),
                        new Icons().generate(
                                CassandraDataControl
                                        .getInstance()
                                        .getRowFromTabletsKeyspace(
                                                EntitiesInstances.ICONS,
                                                "policeType",
                                                trackerInfo.getPatrul().getPoliceType()
                                        )
                        )
                ),
                icons -> {
                    this.setIcon( icons.getIcon1() );
                    this.setIcon2( icons.getIcon2() );
                }
        );
    }

    public LastPosition (
            final TrackerInfo trackerInfo,
            final Patrul patrul
    ) {
        super.checkAndSetParams(
                trackerInfo,
                trackerInfo1 -> {
                    this.setCarType( trackerInfo1.getTupleOfCar().getCarModel() );
                    this.setTrackerId( trackerInfo1.getTupleOfCar().getTrackerId() );
                    this.setLastLatitude( trackerInfo1.getTupleOfCar().getLatitude() );
                    this.setCarGosNumber( trackerInfo1.getTupleOfCar().getGosNumber() );
                    this.setLastLongitude( trackerInfo1.getTupleOfCar().getLongitude() );
                }
        );

        super.checkAndSetParams(
                patrul,
                patrul1 -> {
                    this.setPatrulUUID( patrul1.getUuid() );
                    this.setPoliceType( patrul1.getPoliceType() );
                    this.setStatus( patrul1.getPatrulTaskInfo().getStatus() );
                    this.setTaskId( patrul1.getPatrulTaskInfo().getTaskId() );
                    this.setPatrulName( patrul1.getPatrulFIOData().getName() );
                    this.setPatrulpassportSeries( patrul1.getPassportNumber() );
                }
        );
    }
}
