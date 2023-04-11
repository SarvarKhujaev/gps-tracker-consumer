package com.ssd.mvd.entity;

import com.ssd.mvd.database.CassandraDataControl;
import com.ssd.mvd.constants.Status;
import java.util.UUID;

@lombok.Data
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

        Icons icons = CassandraDataControl
                .getInstance()
                .getGetPoliceType()
                .apply( trackerInfo.getPatrul().getPoliceType() );
        this.setIcon( icons.getIcon1() );
        this.setIcon2( icons.getIcon2() );
        this.setTaskId( trackerInfo.getPatrul().getTaskId() );
        this.setStatus( trackerInfo.getPatrul().getStatus() );
        this.setPatrulName( trackerInfo.getPatrul().getName() );
        this.setPatrulUUID( trackerInfo.getPatrul().getUuid() );
        this.setPoliceType( trackerInfo.getPatrul().getPoliceType() );
        this.setPatrulpassportSeries( trackerInfo.getPatrul().getPassportNumber() ); }

    public LastPosition ( TrackerInfo trackerInfo, Patrul patrul ) {
        this.setCarType( trackerInfo.getTupleOfCar().getCarModel() );
        this.setTrackerId( trackerInfo.getTupleOfCar().getTrackerId() );
        this.setLastLatitude( trackerInfo.getTupleOfCar().getLatitude() );
        this.setCarGosNumber( trackerInfo.getTupleOfCar().getGosNumber() );
        this.setLastLongitude( trackerInfo.getTupleOfCar().getLongitude() );

        if ( patrul != null ) {
            this.setTaskId( patrul.getTaskId() );
            this.setStatus( patrul.getStatus() );
            this.setPatrulUUID( patrul.getUuid() );
            this.setPatrulName( patrul.getName() );
            this.setPoliceType( patrul.getPoliceType() );
            this.setPatrulpassportSeries( patrul.getPassportNumber() ); } }
}
