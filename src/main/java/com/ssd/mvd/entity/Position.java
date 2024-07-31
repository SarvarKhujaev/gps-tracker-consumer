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

import java.text.MessageFormat;
import java.util.Date;
import java.util.UUID;

public final class Position
        extends StringOperations
        implements EntityToCassandraConverter,
        KafkaEntitiesCommonMethods {
    public void update (
            final Patrul patrul
    ) {
        this.setLongitudeOfTask( patrul.getPatrulLocationData().getLongitudeOfTask() );
        this.setLatitudeOfTask( patrul.getPatrulLocationData().getLatitudeOfTask() );

        this.setPatrulName( patrul.getPatrulFIOData().getName() );

        this.setTaskId( patrul.getPatrulTaskInfo().getTaskId() );
        this.setStatus( patrul.getPatrulTaskInfo().getStatus() );

        this.setPoliceType( patrul.getPoliceType() );
        this.setPatrulUUID( patrul.getUuid() );

        this.setDistrictId( patrul.getPatrulRegionData().getDistrictId() );
        this.setMahallaId( patrul.getPatrulRegionData().getMahallaId() );
        this.setRegionId( patrul.getPatrulRegionData().getRegionId() );
    }

    public void update (
            final Icons icons
    ) {
        this.setIcon( icons.getIcon1() );
        this.setIcon2( icons.getIcon2() );
    }

    public void setIcon( final String icon ) {
        this.icon = icon;
    }

    public void setIcon2( final String icon2 ) {
        this.icon2 = icon2;
    }

    public void setCarType( final String carType ) {
        this.carType = carType;
    }

    public void setCarGosNumber( final String carGosNumber ) {
        this.carGosNumber = carGosNumber;
    }

    public void setTaskId( final String taskId ) {
        this.taskId = taskId;
    }

    public void setPatrulName( final String patrulName ) {
        this.patrulName = patrulName;
    }

    public void setPoliceType( final String policeType ) {
        this.policeType = policeType;
    }

    public Status getStatus() {
        return this.status;
    }

    public void setStatus( final Status status ) {
        this.status = status;
    }

    public void setPatrulUUID( final UUID patrulUUID ) {
        this.patrulUUID = patrulUUID;
    }

    public void setLatitudeOfTask( final double latitudeOfTask ) {
        this.latitudeOfTask = latitudeOfTask;
    }

    public void setLongitudeOfTask( final double longitudeOfTask ) {
        this.longitudeOfTask = longitudeOfTask;
    }

    public void setRegionId( final long regionId ) {
        this.regionId = regionId;
    }

    public void setMahallaId( final long mahallaId ) {
        this.mahallaId = mahallaId;
    }

    public void setDistrictId( final long districtId ) {
        this.districtId = districtId;
    }

    public String getDeviceId() {
        return this.deviceId;
    }

    public Date getDeviceTime() {
        return this.deviceTime;
    }

    public double getSpeed() {
        return this.speed;
    }

    public double getLatitude() {
        return this.latitude;
    }

    public void setLatitude( final double latitude ) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return this.longitude;
    }

    public void setLongitude( final double longitude ) {
        this.longitude = longitude;
    }

    // only for Car
    private String icon; // иконка патрульного выбирается исходя из типа патрульного
    private String icon2; // иконка патрульного выбирается исходя из типа патрульного
    private String carType;
    private String carGosNumber;

    // only for Patrul
    private String taskId;
    private String patrulName;
    private String policeType;

    private Status status;
    private UUID patrulUUID;

    private long regionId;
    private long mahallaId;
    private long districtId; // choosing from dictionary

    // Tracker data
    private String deviceId;
    private Date deviceTime;

    private double speed;
    private double latitude;
    private double longitude;

    private double latitudeOfTask;
    private double longitudeOfTask;

    @Override
    public CassandraTables getEntityTableName() {
        return CassandraTables.TRACKERS;
    }

    @Override
    public CassandraTables getEntityKeyspaceName() {
        return CassandraTables.TRACKERS_LOCATION_TABLE;
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
