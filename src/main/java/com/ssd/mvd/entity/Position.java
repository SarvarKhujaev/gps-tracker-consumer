package com.ssd.mvd.entity;

import com.ssd.mvd.interfaces.EntityToCassandraConverter;
import com.ssd.mvd.interfaces.KafkaEntitiesCommonMethods;
import com.ssd.mvd.kafka.kafkaConfigs.KafkaTopics;
import com.ssd.mvd.entity.patrulDataSet.Patrul;
import com.ssd.mvd.inspectors.StringOperations;
import com.ssd.mvd.constants.CassandraCommands;
import com.ssd.mvd.constants.CassandraTables;
import com.ssd.mvd.inspectors.Inspector;
import com.ssd.mvd.constants.Status;

import com.google.gson.annotations.Expose;
import java.text.MessageFormat;

import java.util.Date;
import java.util.UUID;

public final class Position
        extends StringOperations
        implements EntityToCassandraConverter, KafkaEntitiesCommonMethods {
    public double getSpeed() {
        return this.speed;
    }

    public Status getStatus() {
        return this.status;
    }

    public String getDeviceId() {
        return this.deviceId;
    }

    public Date getDeviceTime() {
        return this.deviceTime;
    }

    public double getLatitude() {
        return this.latitude;
    }

    public double getLongitude() {
        return this.longitude;
    }

    public void setIcon( final String icon ) {
        this.icon = icon;
    }

    public void setIcon2( final String icon2 ) {
        this.icon2 = icon2;
    }

    public void setTaskId( final String taskId ) {
        this.taskId = taskId;
    }

    public void setStatus( final Status status ) {
        this.status = status;
    }

    public void setCarType( final String carType ) {
        this.carType = carType;
    }

    public void setRegionId( final long regionId ) {
        this.regionId = regionId;
    }

    public void setLatitude( final double latitude ) {
        this.latitude = latitude;
    }

    public void setLongitude( final double longitude ) {
        this.longitude = longitude;
    }

    public void setMahallaId( final long mahallaId ) {
        this.mahallaId = mahallaId;
    }

    public void setPatrulUUID( final UUID patrulUUID ) {
        this.patrulUUID = patrulUUID;
    }

    public void setDistrictId( final long districtId ) {
        this.districtId = districtId;
    }

    public void setPatrulName( final String patrulName ) {
        this.patrulName = patrulName;
    }

    public void setPoliceType( final String policeType ) {
        this.policeType = policeType;
    }

    public void setCarGosNumber( final String carGosNumber ) {
        this.carGosNumber = carGosNumber;
    }

    public void setLatitudeOfTask( final double latitudeOfTask ) {
        this.latitudeOfTask = latitudeOfTask;
    }

    public void setLongitudeOfTask( final double longitudeOfTask ) {
        this.longitudeOfTask = longitudeOfTask;
    }

    public void update (
            final Patrul patrul
    ) {
        this.setLongitudeOfTask( patrul.getPatrulLocationData().getLongitudeOfTask() );
        this.setLatitudeOfTask( patrul.getPatrulLocationData().getLatitudeOfTask() );

        this.setDistrictId( patrul.getPatrulRegionData().getDistrictId() );
        this.setMahallaId( patrul.getPatrulRegionData().getMahallaId() );
        this.setRegionId( patrul.getPatrulRegionData().getRegionId() );

        this.setPatrulName( patrul.getPatrulFIOData().getName() );

        this.setTaskId( patrul.getPatrulTaskInfo().getTaskId() );
        this.setStatus( patrul.getPatrulTaskInfo().getStatus() );

        this.setPoliceType( patrul.getPoliceType() );
        this.setPatrulUUID( patrul.getUuid() );
    }

    public void update (
            final Icons icons
    ) {
        this.setIcon( icons.getIcon1() );
        this.setIcon2( icons.getIcon2() );
    }

    // only for Car
    @Expose
    private String icon; // иконка патрульного выбирается исходя из типа патрульного
    @Expose
    private String icon2; // иконка патрульного выбирается исходя из типа патрульного
    @Expose
    private String carType;
    @Expose
    private String carGosNumber;

    // only for Patrul
    @Expose
    private String taskId;
    @Expose
    private String patrulName;
    @Expose
    private String policeType;

    @Expose
    private Status status;
    @Expose
    private UUID patrulUUID;

    @Expose
    private long regionId;
    @Expose
    private long mahallaId;
    @Expose
    private long districtId;

    // Tracker data
    @Expose
    private String deviceId;
    @Expose
    private Date deviceTime;

    @Expose
    private double speed;
    @Expose
    private double latitude;
    @Expose
    private double longitude;

    @Expose
    private double latitudeOfTask;
    @Expose
    private double longitudeOfTask;

    @Override
    public CassandraTables getEntityKeyspaceName() {
        return CassandraTables.TRACKERS_LOCATION_TABLE;
    }

    @Override
    public CassandraTables getEntityTableName() {
        return CassandraTables.TRACKERS;
    }

    @Override
    public String getEntityUpdateCommand () {
        return MessageFormat.format(
                """
                {0} {1}.{2}
                ( imei, date, speed, altitude, longitude, address )
                VALUES ( {3}, {4}, {5}, {6}, {7}, '' );
                """,
                CassandraCommands.INSERT_INTO,

                CassandraTables.ESCORT,
                CassandraTables.ESCORT_LOCATION,

                super.joinWithAstrix( this.getDeviceId() ),
                super.joinWithAstrix( this.getDeviceTime() ),

                this.getSpeed(),
                this.getLongitude(),
                this.getLatitude()
        );
    }

    @Override
    public String getEntityInsertCommand() {
        return MessageFormat.format(
                """
                {0} {1}.{2}
                ( imei, date, speed, latitude, longitude, address )
                VALUES ( {3}, {4}, {5}, {6}, {7}, '' )
                """,
                CassandraCommands.INSERT_INTO,

                this.getEntityKeyspaceName(),
                this.getEntityTableName(),

                super.joinWithAstrix( this.getDeviceId() ),
                super.joinWithAstrix( this.getDeviceTime() ),

                this.getSpeed(),
                this.getLongitude(),
                this.getLatitude()
        );
    }

    @Override
    public KafkaTopics getTopicName() {
        return Inspector.trackerInfoMap.containsKey( this.getDeviceId() )
                ? KafkaTopics.WEBSOCKET_SERVICE_TOPIC_FOR_ONLINE
                : KafkaTopics.TUPLE_OF_CAR_LOCATION_TOPIC;
    }

    @Override
    public String getSuccessMessage() {
        return String.join(
                " ",
                (
                        Inspector.trackerInfoMap.containsKey( this.getDeviceId() )
                                ? "Kafka got patrul car:"
                                : "Kafka got Escort car location:"
                ),
                this.getDeviceId(),
                "at:",
                this.getDeviceTime().toString()
        );
    }
}
