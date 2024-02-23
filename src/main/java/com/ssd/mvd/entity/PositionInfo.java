package com.ssd.mvd.entity;

import java.util.Date;
import com.datastax.driver.core.Row;
import com.ssd.mvd.controller.UnirestController;

// хранит исторические данные о передвижениях машины
public final class PositionInfo {
    public Double getLat() {
        return this.lat;
    }

    public void setLat( final Double lat ) {
        this.lat = lat;
    }

    public Double getLng() {
        return this.lng;
    }

    public void setLng( final Double lng ) {
        this.lng = lng;
    }

    public Double getSpeed() {
        return this.speed;
    }

    public void setSpeed( final Double speed ) {
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

    private Double lat;
    private Double lng;
    private Double speed;

    private String address;
    private Date positionWasSavedDate;

    public PositionInfo ( final Row row, final Boolean flag ) {
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
}
