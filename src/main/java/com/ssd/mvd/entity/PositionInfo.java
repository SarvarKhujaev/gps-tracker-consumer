package com.ssd.mvd.entity;

import com.datastax.driver.core.Row;
import lombok.Data;

@Data // used in case of historical request for some time duration
public class PositionInfo {
    private Double lat;
    private Double lng;

    public PositionInfo ( Row row ) {
        this.setLng( row.getDouble( "latitude" ) );
        this.setLat( row.getDouble( "longitude" ) ); }
}
