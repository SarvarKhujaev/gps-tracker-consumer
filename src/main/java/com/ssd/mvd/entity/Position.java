package com.ssd.mvd.entity;

import com.ssd.mvd.constants.Status;
import lombok.Data;

import java.util.Date;
import java.util.UUID;

@Data
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

    private long id;

    private String deviceId;

    public String getDeviceId() {
        return deviceId;
    }

    private String type;

    private String protocol;

    private Date serverTime;

    private Date deviceTime;

    private Date fixTime;

    private boolean outdated;

    private boolean valid;

    private double latitude;

    public double getLatitude() {
        return latitude;
    }

    private double longitude;

    public double getLongitude() {
        return longitude;
    }

    private double altitude;

    private double speed; // value in knots

    public double getSpeed() {
        return speed;
    }

    private double course;

    private String address;

    private int port;

    private int isLine;
}
