package com.ssd.mvd.entity;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.core.CqlIdentifier;

import com.ssd.mvd.annotations.AvroFieldAnnotation;
import com.ssd.mvd.annotations.AvroMethodAnnotation;
import com.ssd.mvd.interfaces.EntityToCassandraConverter;
import com.ssd.mvd.interfaces.KafkaEntitiesCommonMethods;

import com.ssd.mvd.annotations.EntityAnnotations;
import com.ssd.mvd.annotations.FieldAnnotation;

import com.ssd.mvd.kafka.kafkaConfigs.KafkaTopics;
import com.ssd.mvd.entity.patrulDataSet.Patrul;

import com.ssd.mvd.inspectors.StringOperations;
import com.ssd.mvd.inspectors.Inspector;

import com.ssd.mvd.constants.CassandraCommands;
import com.ssd.mvd.constants.CassandraTables;
import com.ssd.mvd.constants.Status;
import org.apache.avro.Schema;

import java.text.MessageFormat;
import java.util.Date;
import java.util.UUID;

@EntityAnnotations( name = "Position", comment = "Данные о позиции патрульной машины" )
public final class Position implements EntityToCassandraConverter, KafkaEntitiesCommonMethods {
    @AvroMethodAnnotation( name = "speed" )
    public double getSpeed() {
        return this.speed;
    }

    @AvroMethodAnnotation( name = "deviceId" )
    public String getDeviceId() {
        return this.deviceId;
    }

    @AvroMethodAnnotation( name = "deviceTime" )
    public Date getDeviceTime() {
        return this.deviceTime;
    }

    @AvroMethodAnnotation( name = "latitude" )
    public double getLatitude() {
        return this.latitude;
    }

    @AvroMethodAnnotation( name = "longitude" )
    public double getLongitude() {
        return this.longitude;
    }

    @AvroMethodAnnotation( name = "icon" )
    public String getIcon() {
        return this.icon;
    }

    @AvroMethodAnnotation( name = "icon2" )
    public String getIcon2() {
        return this.icon2;
    }

    @AvroMethodAnnotation( name = "carType" )
    public String getCarType() {
        return this.carType;
    }

    @AvroMethodAnnotation( name = "carGosNumber" )
    public String getCarGosNumber() {
        return this.carGosNumber;
    }

    @AvroMethodAnnotation( name = "taskId" )
    public String getTaskId() {
        return this.taskId;
    }

    @AvroMethodAnnotation( name = "patrulName" )
    public String getPatrulName() {
        return this.patrulName;
    }

    @AvroMethodAnnotation( name = "policeType" )
    public String getPoliceType() {
        return this.policeType;
    }

    @AvroMethodAnnotation( name = "status" )
    public Status getStatus() {
        return this.status;
    }

    @AvroMethodAnnotation( name = "patrulUUID" )
    public UUID getPatrulUUID() {
        return this.patrulUUID;
    }

    @AvroMethodAnnotation( name = "regionId" )
    public long getRegionId() {
        return this.regionId;
    }

    @AvroMethodAnnotation( name = "mahallaId" )
    public long getMahallaId() {
        return this.mahallaId;
    }

    @AvroMethodAnnotation( name = "districtId" )
    public long getDistrictId() {
        return this.districtId;
    }

    @AvroMethodAnnotation( name = "latitudeOfTask" )
    public double getLatitudeOfTask() {
        return this.latitudeOfTask;
    }

    @AvroMethodAnnotation( name = "longitudeOfTask" )
    public double getLongitudeOfTask() {
        return this.longitudeOfTask;
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
            @lombok.NonNull final Patrul patrul
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
            @lombok.NonNull final Icons icons
    ) {
        this.setIcon( icons.getIcon1() );
        this.setIcon2( icons.getIcon2() );
    }

    @FieldAnnotation(
            name = "icon",
            comment = "иконка патрульного выбирается исходя из типа патрульного",
            hasToBeJoinedWithAstrix = true
    )
    @AvroFieldAnnotation( name = "icon" )
    private String icon;

    @FieldAnnotation(
            name = "icon2",
            comment = "иконка патрульного выбирается исходя из типа патрульного",
            hasToBeJoinedWithAstrix = true
    )
    @AvroFieldAnnotation( name = "icon2" )
    private String icon2;

    @FieldAnnotation( name = "carType", hasToBeJoinedWithAstrix = true )
    @AvroFieldAnnotation( name = "carType" )
    private String carType;

    @FieldAnnotation( name = "carGosNumber", hasToBeJoinedWithAstrix = true )
    @AvroFieldAnnotation( name = "carGosNumber" )
    private String carGosNumber;

    @FieldAnnotation( name = "taskId", hasToBeJoinedWithAstrix = true )
    @AvroFieldAnnotation( name = "taskId" )
    private String taskId;

    @FieldAnnotation( name = "patrulName", hasToBeJoinedWithAstrix = true )
    @AvroFieldAnnotation( name = "patrulName" )
    private String patrulName;

    @FieldAnnotation( name = "policeType", hasToBeJoinedWithAstrix = true )
    @AvroFieldAnnotation( name = "policeType" )
    private String policeType;

    @FieldAnnotation( name = "deviceId", hasToBeJoinedWithAstrix = true, mightBeNull = false )
    @AvroFieldAnnotation( name = "deviceId" )
    private String deviceId;

    @FieldAnnotation( name = "status", hasToBeJoinedWithAstrix = true, mightBeNull = false )
    @AvroFieldAnnotation(
            name = "status",
            isEnum = true,
            chosenEnum = 2,
            schemaType = Schema.Type.ENUM
    )
    private Status status;

    @FieldAnnotation( name = "patrulUUID", mightBeNull = false )
    @AvroFieldAnnotation( name = "patrulUUID", schemaType = Schema.Type.STRING )
    private UUID patrulUUID;

    @FieldAnnotation( name = "regionId" )
    @AvroFieldAnnotation( name = "regionId", schemaType = Schema.Type.LONG )
    private long regionId;
    @FieldAnnotation( name = "mahallaId" )
    @AvroFieldAnnotation( name = "mahallaId", schemaType = Schema.Type.LONG )
    private long mahallaId;
    @FieldAnnotation( name = "districtId" )
    @AvroFieldAnnotation( name = "districtId", schemaType = Schema.Type.LONG )
    private long districtId;

    @FieldAnnotation( name = "deviceTime", hasToBeJoinedWithAstrix = true, mightBeNull = false )
    @AvroFieldAnnotation( name = "deviceTime", isDate = true )
    private Date deviceTime;

    @FieldAnnotation( name = "speed", mightBeNull = false )
    @AvroFieldAnnotation( name = "speed", schemaType = Schema.Type.DOUBLE )
    private double speed;

    @FieldAnnotation( name = "latitude" )
    @AvroFieldAnnotation( name = "latitude", schemaType = Schema.Type.DOUBLE )
    private double latitude;

    @FieldAnnotation( name = "longitude" )
    @AvroFieldAnnotation( name = "longitude", schemaType = Schema.Type.DOUBLE )
    private double longitude;

    @FieldAnnotation( name = "latitudeOfTask" )
    @AvroFieldAnnotation( name = "latitudeOfTask", schemaType = Schema.Type.DOUBLE )
    private double latitudeOfTask;

    @FieldAnnotation( name = "longitudeOfTask" )
    @AvroFieldAnnotation( name = "longitudeOfTask", schemaType = Schema.Type.DOUBLE )
    private double longitudeOfTask;

    @Override
    @lombok.NonNull
    public String getEntityUpdateCommand () {
        return MessageFormat.format(
                """
                {0} {1}.{2}
                ( imei, date, speed, altitude, longitude, address )
                VALUES ( {3}, {4}, {5}, {6}, {7}, {8} );
                """,
                CassandraCommands.INSERT_INTO,

                CassandraTables.ESCORT,
                CassandraTables.ESCORT_LOCATION,

                StringOperations.joinWithAstrix( this.getDeviceId() ),
                StringOperations.joinWithAstrix( this.getDeviceTime() ),

                this.getSpeed(),
                this.getLongitude(),
                this.getLatitude(),

                StringOperations.EMPTY
        );
    }

    @Override
    @lombok.NonNull
    public Insert getEntityInsert() {
        return QueryBuilder.insertInto(
                this.getEntityKeyspaceName().name(),
                this.getEntityTableName().name()
        ).value( CqlIdentifier.fromCql( "imei" ), literal( this.getDeviceId() ) )
                .value( CqlIdentifier.fromCql( "date" ), literal( this.getDeviceTime() ) )
                .value( CqlIdentifier.fromCql( "speed" ), literal( this.getSpeed() ) )
                .value( CqlIdentifier.fromCql( "latitude" ), literal( this.getLatitude() ) )
                .value( CqlIdentifier.fromCql( "longitude" ), literal( this.getLongitude() ) );
    }

    @Override
    @lombok.NonNull
    public KafkaTopics getTopicName() {
        return KafkaTopics.TUPLE_OF_CAR_LOCATION_TOPIC;
    }

    @Override
    @lombok.NonNull
    public String getSuccessMessage() {
        return String.join(
                StringOperations.SPACE,
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
