package com.ssd.mvd.entity;

import java.util.Date;
import com.datastax.driver.core.Row;
import com.ssd.mvd.controller.UnirestController;

@lombok.Data // хранит исторические данные о передвижениях машины
public final class PositionInfo {
    private Double lat;
    private Double lng;
    private Double speed;

    private String address;
    private Date positionWasSavedDate;

    public PositionInfo ( final Row row, final Boolean flag ) {
        this.setSpeed( row.getDouble( "speed" ) );
        this.setLng( row.getDouble( "latitude" ) );
        this.setLat( row.getDouble( "longitude" ) );
        if ( flag ) this.setAddress( UnirestController
                .getInstance()
                .getGetAddressByLocation()
                .apply( this.getLat(), this.getLng() ) );
        this.setPositionWasSavedDate( row.getTimestamp( "date" ) ); }
}
