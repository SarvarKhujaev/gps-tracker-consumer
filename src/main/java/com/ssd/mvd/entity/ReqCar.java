package com.ssd.mvd.entity;

import com.datastax.driver.core.Row;
import java.util.Optional;
import java.util.UUID;

@lombok.Data
@lombok.NoArgsConstructor
@lombok.AllArgsConstructor
public final class ReqCar {
    private UUID uuid;

    private String gosNumber;
    private String trackerId;
    private String vehicleType;
    private String patrulPassportSeries;

    private Double latitude;
    private Double longitude;
    private Double averageFuelSize; // средний расход топлива по документам

    public ReqCar ( final Row row ) { Optional.ofNullable( row ).ifPresent( row1 -> {
            this.setUuid( row.getUUID( "uuid" ) );

            this.setGosNumber( row.getString( "gosNumber" ) );
            this.setTrackerId( row.getString( "trackerId" ) );
            this.setVehicleType( row.getString( "vehicleType" ) );
            this.setPatrulPassportSeries( row.getString( "patrulPassportSeries" ) );

            this.setLatitude( row.getDouble( "latitude" ) );
            this.setLongitude( row.getDouble( "longitude" ) );
            this.setAverageFuelSize( row.getDouble( "averageFuelSize" ) ); } ); }
}
