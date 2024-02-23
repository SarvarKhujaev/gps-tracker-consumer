package com.ssd.mvd.entity.patrulDataSet;

import com.datastax.driver.core.Row;
import com.ssd.mvd.entity.TupleOfCar;

import java.util.Optional;
import java.util.UUID;

public final class Patrul {
    public UUID getUuid() {
        return this.uuid;
    }

    public void setUuid( final UUID uuid ) {
        this.uuid = uuid;
    }

    public String getPoliceType() {
        return this.policeType;
    }

    public void setPoliceType( final String policeType ) {
        this.policeType = policeType;
    }

    public String getPassportNumber() {
        return this.passportNumber;
    }

    public void setPassportNumber( final String passportNumber ) {
        this.passportNumber = passportNumber;
    }

    public PatrulFIOData getPatrulFIOData() {
        return this.patrulFIOData;
    }

    public void setPatrulFIOData( final PatrulFIOData patrulFIOData ) {
        this.patrulFIOData = patrulFIOData;
    }

    public PatrulCarInfo getPatrulCarInfo() {
        return this.patrulCarInfo;
    }

    public void setPatrulCarInfo( final PatrulCarInfo patrulCarInfo ) {
        this.patrulCarInfo = patrulCarInfo;
    }

    public PatrulTaskInfo getPatrulTaskInfo() {
        return this.patrulTaskInfo;
    }

    public void setPatrulTaskInfo( final PatrulTaskInfo patrulTaskInfo ) {
        this.patrulTaskInfo = patrulTaskInfo;
    }

    public PatrulRegionData getPatrulRegionData() {
        return this.patrulRegionData;
    }

    public void setPatrulRegionData( final PatrulRegionData patrulRegionData ) {
        this.patrulRegionData = patrulRegionData;
    }

    public PatrulLocationData getPatrulLocationData() {
        return this.patrulLocationData;
    }

    public void setPatrulLocationData( final PatrulLocationData patrulLocationData ) {
        this.patrulLocationData = patrulLocationData;
    }

    public PatrulUniqueValues getPatrulUniqueValues() {
        return this.patrulUniqueValues;
    }

    public void setPatrulUniqueValues( final PatrulUniqueValues patrulUniqueValues ) {
        this.patrulUniqueValues = patrulUniqueValues;
    }

    public void linkWithTupleOfCar ( final TupleOfCar tupleOfCar ) {
        this.getPatrulUniqueValues().setUuidForEscortCar( tupleOfCar.getUuid() );
        this.getPatrulCarInfo().setCarNumber( tupleOfCar );
    }

    private Double latitudeOfTask;
    private Double longitudeOfTask;

    private UUID uuid;
    private UUID uuidForEscortCar;

    private String policeType;
    private String passportNumber;

    private PatrulFIOData patrulFIOData;
    private PatrulCarInfo patrulCarInfo;
    private PatrulTaskInfo patrulTaskInfo;
    private PatrulRegionData patrulRegionData;
    private PatrulLocationData patrulLocationData;
    private PatrulUniqueValues patrulUniqueValues;

    public Patrul ( final Row row ) {
        Optional.of( row ).ifPresent( row1 -> {
            this.setUuid( row.getUUID( "uuid" ) );
            this.setPoliceType( row.getString( "policeType" ) );
            this.setPassportNumber( row.getString( "passportNumber" ) );

            this.setPatrulFIOData( PatrulFIOData.generate( row ) );
            this.setPatrulCarInfo( PatrulCarInfo.generate( row ) );
            this.setPatrulTaskInfo( PatrulTaskInfo.generate( row ) );
            this.setPatrulRegionData( PatrulRegionData.generate( row ) );
            this.setPatrulLocationData( PatrulLocationData.generate( row ) );
            this.setPatrulUniqueValues( PatrulUniqueValues.generate( row ) );
        } );
    }
}
