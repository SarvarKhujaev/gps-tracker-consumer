package com.ssd.mvd.entity;

import com.datastax.driver.core.Row;
import java.util.Optional;
import java.util.UUID;

public final class ReqCar {
    public UUID getUuid() {
        return this.uuid;
    }

    public void setUuid ( final UUID uuid ) {
        this.uuid = uuid;
    }

    public String getGosNumber() {
        return this.gosNumber;
    }

    public void setGosNumber ( final String gosNumber ) {
        this.gosNumber = gosNumber;
    }

    public String getTrackerId() {
        return this.trackerId;
    }

    public void setTrackerId ( final String trackerId ) {
        this.trackerId = trackerId;
    }

    public String getVehicleType() {
        return this.vehicleType;
    }

    public void setVehicleType ( final String vehicleType ) {
        this.vehicleType = vehicleType;
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

    public Double getAverageFuelSize() {
        return this.averageFuelSize;
    }

    public void setAverageFuelSize ( final Double averageFuelSize ) {
        this.averageFuelSize = averageFuelSize;
    }

    private UUID uuid;

    private String gosNumber;
    private String trackerId;
    private String vehicleType;
    private String patrulPassportSeries;

    private Double latitude;
    private Double longitude;
    private Double averageFuelSize; // средний расход топлива по документам

    public ReqCar ( final Row row ) {
        Optional.ofNullable( row ).ifPresent( row1 -> {
            this.setUuid( row.getUUID( "uuid" ) );

            this.setGosNumber( row.getString( "gosNumber" ) );
            this.setTrackerId( row.getString( "trackerId" ) );
            this.setVehicleType( row.getString( "vehicleType" ) );
            this.setPatrulPassportSeries( row.getString( "patrulPassportSeries" ) );

            this.setLatitude( row.getDouble( "latitude" ) );
            this.setLongitude( row.getDouble( "longitude" ) );
            this.setAverageFuelSize( row.getDouble( "averageFuelSize" ) );
        } );
    }
}
