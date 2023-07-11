package com.ssd.mvd.entity;

import com.datastax.driver.core.Row;
import java.util.Optional;
import java.util.UUID;

@lombok.Data
@lombok.NoArgsConstructor
@lombok.AllArgsConstructor
public final class TupleOfCar {
    private UUID uuid;
    private UUID uuidOfEscort; // UUID of the Escort which this car is linked to
    private UUID uuidOfPatrul;

    private String carModel;
    private String gosNumber;
    private String trackerId;
    private String nsfOfPatrul;
    private String simCardNumber;

    private Double latitude;
    private Double longitude;
    private Double averageFuelConsumption;

    public TupleOfCar ( final Row row ) { Optional.ofNullable( row ).ifPresent( row1 -> {
            this.setUuid( row.getUUID( "uuid" ) );
            this.setUuidOfEscort( row.getUUID( "uuidOfEscort" ) );
            this.setUuidOfPatrul( row.getUUID( "uuidOfPatrul" ) );

            this.setCarModel( row.getString( "carModel" ) );
            this.setGosNumber( row.getString( "gosNumber" ) );
            this.setTrackerId( row.getString( "trackerId" ) );
            this.setNsfOfPatrul( row.getString( "nsfOfPatrul" ) );
            this.setSimCardNumber( row.getString( "simCardNumber" ) );

            this.setLatitude( row.getDouble( "latitude" ) );
            this.setLongitude( row.getDouble( "longitude" ) );
            this.setAverageFuelConsumption( row.getDouble( "averageFuelConsumption" ) ); } ); }
}
