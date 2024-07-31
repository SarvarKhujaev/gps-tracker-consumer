package com.ssd.mvd.entity;

import com.datastax.driver.core.Row;
import com.ssd.mvd.controller.UnirestController;
import com.ssd.mvd.inspectors.DataValidateInspector;
import com.ssd.mvd.interfaces.ObjectFromRowConvertInterface;

import java.util.Date;

// хранит исторические данные о передвижениях машины
public final class PositionInfo
        extends DataValidateInspector
        implements ObjectFromRowConvertInterface< PositionInfo > {
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

    public PositionInfo () {}

    public PositionInfo (
            final Row row,
            final boolean flag
    ) {
        this.generate( row );

        if ( flag ) {
            this.setAddress(
                    UnirestController
                            .getInstance()
                            .getAddressByLocation
                            .apply( this.getLat(), this.getLng() )
            );
        }
    }

    @Override
    public PositionInfo generate( final Row row ) {
        super.checkAndSetParams(
                row,
                row1 -> {
                    this.setSpeed( row.getDouble( "speed" ) );
                    this.setLng( row.getDouble( "latitude" ) );
                    this.setLat( row.getDouble( "longitude" ) );
                    this.setPositionWasSavedDate( row.getTimestamp( "date" ) );
                }
        );

        return this;
    }

    @Override
    public PositionInfo generate() {
        return new PositionInfo();
    }
}
