package com.ssd.mvd.entity;

import com.ssd.mvd.inspectors.DataValidateInspector;
import com.ssd.mvd.constants.Status;

import com.datastax.driver.core.Row;
import java.util.UUID;

@lombok.Data
public class Patrul {
    private Double latitudeOfTask;
    private Double longitudeOfTask;

    private UUID uuid; // own id of the patrul
    private UUID uuidForEscortCar; // choosing from dictionary

    private String name;
    private String taskId;
    private String carType; // модель машины
    private String carNumber;
    private String policeType; // choosing from dictionary
    private String passportNumber;

    private Long regionId;
    private Long mahallaId;
    private Long districtId; // choosing from dictionary

    private Status status; // busy, free by default, available or not available

    public Patrul ( final Row row ) {
        if ( DataValidateInspector
                .getInstance()
                .checkParam
                .test( row ) ) {
            this.setUuid( row.getUUID( "uuid" ) );
            this.setStatus( Status.valueOf( row.getString( "status" ) ) );

            this.setRegionId( row.getLong( "regionId" ) );
            this.setMahallaId( row.getLong( "mahallaId" ) );
            this.setDistrictId( row.getLong( "districtId" ) );

            this.setLatitudeOfTask( row.getDouble( "latitudeOfTask" ) );
            this.setLongitudeOfTask( row.getDouble( "longitudeOfTask" ) );

            this.setName( row.getString( "name" ) );
            this.setTaskId( row.getString( "taskId" ) );
            this.setCarType( row.getString( "carType" ) );
            this.setCarNumber( row.getString( "carNumber" ) );
            this.setPoliceType( row.getString( "policeType" ) );
            this.setPassportNumber( row.getString( "passportNumber" ) ); } }
}
