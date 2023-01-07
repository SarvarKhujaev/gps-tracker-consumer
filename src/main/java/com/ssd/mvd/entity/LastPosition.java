package com.ssd.mvd.entity;

import com.ssd.mvd.constants.Status;
import java.util.UUID;
import lombok.Data;

@Data
public class LastPosition {
    // Car data
    private String icon; // иконка патрульного выбирается исходя из типа патрульного
    private String icon2; // иконка патрульного выбирается исходя из типа патрульного
    private String carType;
    private String trackerId;
    private String carGosNumber;

    private Double lastLatitude;
    private Double lastLongitude;

    // Patrul data
    private UUID patrulUUID;
    private String taskId;
    private Status status;
    private String patrulName;
    private String policeType;
    private String patrulpassportSeries;

    public LastPosition ( TrackerInfo trackerInfo ) {
        this.setCarType( trackerInfo.getReqCar().getVehicleType() );
        this.setTrackerId( trackerInfo.getReqCar().getTrackerId() );
        this.setLastLatitude( trackerInfo.getReqCar().getLatitude() );
        this.setCarGosNumber( trackerInfo.getReqCar().getGosNumber() );
        this.setLastLongitude( trackerInfo.getReqCar().getLongitude() );

        this.setIcon( trackerInfo.getIcon() );
        this.setIcon2( trackerInfo.getIcon2() );
        this.setTaskId( trackerInfo.getPatrul().getTaskId() );
        this.setStatus( trackerInfo.getPatrul().getStatus() );
        this.setPatrulName( trackerInfo.getPatrul().getName() );
        this.setPatrulUUID( trackerInfo.getPatrul().getUuid() );
        this.setPoliceType( trackerInfo.getPatrul().getPoliceType() );
        this.setPatrulpassportSeries( trackerInfo.getPatrul().getPassportNumber() ); }

    public LastPosition ( TrackerInfo trackerInfo, Boolean check ) {
        this.setCarType( trackerInfo.getTupleOfCar().getCarModel() );
        this.setTrackerId( trackerInfo.getTupleOfCar().getTrackerId() );
        this.setLastLatitude( trackerInfo.getTupleOfCar().getLatitude() );
        this.setCarGosNumber( trackerInfo.getTupleOfCar().getGosNumber() );
        this.setLastLongitude( trackerInfo.getTupleOfCar().getLongitude() );

        this.setTaskId( trackerInfo.getPatrul() != null ? trackerInfo.getPatrul().getTaskId() : null );
        this.setStatus( trackerInfo.getPatrul() != null ? trackerInfo.getPatrul().getStatus() : null );
        this.setPatrulUUID( trackerInfo.getPatrul() != null ? trackerInfo.getPatrul().getUuid() : null );
        this.setPatrulName( trackerInfo.getPatrul() != null ? trackerInfo.getPatrul().getName() : null );
        this.setPoliceType( trackerInfo.getPatrul() != null ? trackerInfo.getPatrul().getPoliceType() : null );
        this.setPatrulpassportSeries( trackerInfo.getPatrul() != null ? trackerInfo.getPatrul().getPassportNumber() : null ); }
}
