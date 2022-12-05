package com.ssd.mvd.entity;

import com.datastax.driver.core.Row;
import java.util.Date;
import lombok.Data;

@Data // used in case of historical request for some time duration
public class PositionInfo {
    private Double lat;
    private Double lng;
    private String address;
    private Date positionWasSavedDate;

    public PositionInfo ( Row row ) {
        this.setLng( row.getDouble( "latitude" ) );
        this.setLat( row.getDouble( "longitude" ) );
        this.setAddress( row.getString( "address" ) );
        this.setPositionWasSavedDate( row.getTimestamp( "date" ) ); }
}
