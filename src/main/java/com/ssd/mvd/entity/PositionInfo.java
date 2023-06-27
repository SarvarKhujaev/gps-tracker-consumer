package com.ssd.mvd.entity;

import com.datastax.driver.core.Row;
import java.util.Date;

@lombok.Data // хранит исторические данные о передвижениях машины
public final class PositionInfo {
    private Double lat;
    private Double lng;
    private Double speed;

    private String address;
    private Date positionWasSavedDate;

    public PositionInfo ( final Row row ) {
        this.setSpeed( row.getDouble( "speed" ) );
        this.setLng( row.getDouble( "latitude" ) );
        this.setLat( row.getDouble( "longitude" ) );
        this.setAddress( row.getString( "address" ) );
        this.setPositionWasSavedDate( row.getTimestamp( "date" ) ); }
}
