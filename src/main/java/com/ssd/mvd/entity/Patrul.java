package com.ssd.mvd.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.datastax.driver.core.Row;
import com.ssd.mvd.constants.Status;

import java.util.UUID;
import java.util.Map;

@lombok.Data
@lombok.NoArgsConstructor
@lombok.AllArgsConstructor
@JsonIgnoreProperties( ignoreUnknown = true )
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

    private Status status; // busy, free by default, available or not available
    private Map< String, String > listOfTasks; // the list which will store ids of all tasks which have been completed by Patrul

    public Patrul ( Row row ) {
        this.setUuid( row.getUUID( "uuid" ) );
        this.setStatus( Status.valueOf( row.getString( "status" ) ) );

        this.setLatitudeOfTask( row.getDouble( "latitudeOfTask" ) );
        this.setLongitudeOfTask( row.getDouble( "longitudeOfTask" ) );

        this.setName( row.getString( "name" ) );
        this.setTaskId( row.getString( "taskId" ) );
        this.setCarType( row.getString( "carType" ) );
        this.setCarNumber( row.getString( "carNumber" ) );
        this.setPoliceType( row.getString( "policeType" ) );
        this.setPassportNumber( row.getString( "passportNumber" ) ); }
}
