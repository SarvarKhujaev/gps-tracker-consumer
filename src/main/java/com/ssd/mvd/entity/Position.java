package com.ssd.mvd.entity;

import com.ssd.mvd.constants.Status;
import java.util.Date;
import java.util.UUID;

public final class Position {
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

    public String getCarGosNumber() {
        return this.carGosNumber;
    }

    public void setCarGosNumber( final String carGosNumber ) {
        this.carGosNumber = carGosNumber;
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

    public Double getLatitudeOfTask() {
        return this.latitudeOfTask;
    }

    public void setLatitudeOfTask( final Double latitudeOfTask ) {
        this.latitudeOfTask = latitudeOfTask;
    }

    public Double getLongitudeOfTask() {
        return this.longitudeOfTask;
    }

    public void setLongitudeOfTask( final Double longitudeOfTask ) {
        this.longitudeOfTask = longitudeOfTask;
    }

    public Long getRegionId() {
        return this.regionId;
    }

    public void setRegionId( final Long regionId ) {
        this.regionId = regionId;
    }

    public Long getMahallaId() {
        return this.mahallaId;
    }

    public void setMahallaId( final Long mahallaId ) {
        this.mahallaId = mahallaId;
    }

    public Long getDistrictId() {
        return this.districtId;
    }

    public void setDistrictId( final Long districtId ) {
        this.districtId = districtId;
    }

    public String getDeviceId() {
        return this.deviceId;
    }

    public void setDeviceId( final String deviceId ) {
        this.deviceId = deviceId;
    }

    public Date getDeviceTime() {
        return this.deviceTime;
    }

    public void setDeviceTime( final Date deviceTime ) {
        this.deviceTime = deviceTime;
    }

    public Double getSpeed() {
        return this.speed;
    }

    public void setSpeed( final Double speed ) {
        this.speed = speed;
    }

    public Double getLatitude() {
        return this.latitude;
    }

    public void setLatitude( final Double latitude ) {
        this.latitude = latitude;
    }

    public Double getLongitude() {
        return this.longitude;
    }

    public void setLongitude( final Double longitude ) {
        this.longitude = longitude;
    }

    // only for Car
    private String icon; // иконка патрульного выбирается исходя из типа патрульного
    private String icon2; // иконка патрульного выбирается исходя из типа патрульного
    private String carType;
    private String carGosNumber;

    // only for Patrul
    private String taskId;
    private String patrulName;
    private String policeType;

    private Status status;
    private UUID patrulUUID;

    private Double latitudeOfTask;
    private Double longitudeOfTask;

    private Long regionId;
    private Long mahallaId;
    private Long districtId; // choosing from dictionary

    // Tracker data
    private String deviceId;
    private Date deviceTime;

    private Double speed; // value in knots
    private Double latitude;
    private Double longitude;
}
