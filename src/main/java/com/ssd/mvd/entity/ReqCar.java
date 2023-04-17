package com.ssd.mvd.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.datastax.driver.core.Row;
import java.util.UUID;

@lombok.Data
@lombok.NoArgsConstructor
@lombok.AllArgsConstructor
@JsonIgnoreProperties( ignoreUnknown = true )
public class ReqCar {
    private UUID uuid;

    private String gosNumber;
    private String trackerId;
    private String vehicleType;
    private String patrulPassportSeries;

    private Double latitude;
    private Double longitude;
    private Double averageFuelSize; // средний расход топлива по документам

    public ReqCar ( final Row row ) {
        this.setUuid( row.getUUID( "uuid" ) );

        this.setGosNumber( row.getString( "gosNumber" ) );
        this.setTrackerId( row.getString( "trackerId" ) );
        this.setVehicleType( row.getString( "vehicleType" ) );
        this.setPatrulPassportSeries( row.getString( "patrulPassportSeries" ) );

        this.setLatitude( row.getDouble( "latitude" ) );
        this.setLongitude( row.getDouble( "longitude" ) );
        this.setAverageFuelSize( row.getDouble( "averageFuelSize" ) ); }
}
