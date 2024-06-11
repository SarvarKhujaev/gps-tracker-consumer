package com.ssd.mvd.entity;

import java.util.Date;
import com.datastax.driver.core.Row;

import com.ssd.mvd.controller.UnirestController;
import com.ssd.mvd.inspectors.DataValidateInspector;

// хранит исторические данные о передвижениях машины
public final class PositionInfo extends DataValidateInspector {
    public double getLat() {
        return this.lat;
    }

    public void setLat( final double lat ) {
        this.lat = lat;
    }

    public double getLng() {
        return this.lng;
    }

    public void setLng( final double lng ) {
        this.lng = lng;
    }

    public double getSpeed() {
        return this.speed;
    }

    public void setSpeed( final double speed ) {
        this.speed = speed;
    }

    public String getAddress() {
        return this.address;
    }

    public void setAddress( final String address ) {
        this.address = address;
    }

    public Date getPositionWasSavedDate() {
        return this.positionWasSavedDate;
    }

    public void setPositionWasSavedDate( final Date positionWasSavedDate ) {
        this.positionWasSavedDate = positionWasSavedDate;
    }

    private double lat;
    private double lng;
    private double speed;

    private String address;
    private Date positionWasSavedDate;

    public PositionInfo (
            final Row row,
            final boolean flag
    ) {
        super.checkAndSetParams(
                row,
                row1 -> {
                    this.setSpeed( row.getDouble( "speed" ) );
                    this.setLng( row.getDouble( "latitude" ) );
                    this.setLat( row.getDouble( "longitude" ) );
                    if ( flag ) {
                        this.setAddress(
                                UnirestController
                                        .getInstance()
                                        .getAddressByLocation
                                        .apply( this.getLat(), this.getLng() ) );
                    }
                    this.setPositionWasSavedDate( row.getTimestamp( "date" ) );
                }
        );
    }
}
