package com.ssd.mvd.entity;

import com.ssd.mvd.constants.Status;
import java.util.Date;
import java.util.UUID;

@lombok.Data
public class Position {
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

    private String deviceId;

    public String getDeviceId() {
        return deviceId;
    }

    private Date deviceTime;

    private double latitude;

    public double getLatitude() {
        return latitude;
    }

    private double longitude;

    public double getLongitude() {
        return longitude;
    }

    private double speed; // value in knots

    public double getSpeed() { return speed; }
}
