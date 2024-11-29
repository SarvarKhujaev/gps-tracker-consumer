package com.ssd.mvd.entity;

import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.driver.core.BatchStatement;

import com.datastax.oss.driver.api.querybuilder.update.Assignment;
import com.datastax.oss.driver.api.querybuilder.update.Update;

import com.ssd.mvd.annotations.entity.object.EntityConstructorAnnotation;
import com.ssd.mvd.annotations.entity.object.EntityAnnotations;

import com.ssd.mvd.annotations.entity.method.MethodsAnnotations;

import com.ssd.mvd.annotations.entity.field.FieldAnnotation;
import com.ssd.mvd.annotations.entity.field.EntityIndex;

import com.ssd.mvd.annotations.kafka.KafkaEntityAnnotation;

import com.ssd.mvd.interfaces.entity.ObjectFromRowConvertInterface;
import com.ssd.mvd.interfaces.KafkaEntitiesCommonMethods;

import com.ssd.mvd.inspectors.dataTypesInpectors.StringOperations;
import com.ssd.mvd.inspectors.AnnotationInspector;

import com.ssd.mvd.constants.cassandra.CassandraDataTypes;
import com.ssd.mvd.constants.cassandra.CassandraTables;

import com.ssd.mvd.kafka.kafkaConfigs.KafkaTopics;
import com.ssd.mvd.database.CassandraDataControl;
import com.ssd.mvd.inspectors.EntitiesInstances;

import java.util.UUID;

@EntityAnnotations(
        name = "TupleOfCar",
        tableName = CassandraTables.TUPLE_OF_CAR,
        keysapceName = CassandraTables.ESCORT
)
@KafkaEntityAnnotation( topicName = KafkaTopics.NEW_TUPLE_OF_CAR_TOPIC )
public final class TupleOfCar implements ObjectFromRowConvertInterface< TupleOfCar >, KafkaEntitiesCommonMethods {
    @MethodsAnnotations(
            name = "uuid",
            isPrimaryKey = true
    )
    public UUID getUuid () {
        return this.uuid;
    }

    public UUID getUuidOfEscort() {
        return this.uuidOfEscort;
    }

    public UUID getUuidOfPatrul() {
        return this.uuidOfPatrul;
    }

    public String getCarModel() {
        return this.carModel;
    }

    public String getGosNumber() {
        return this.gosNumber;
    }

    public String getTrackerId() {
        return this.trackerId;
    }

    public String getNsfOfPatrul() {
        return this.nsfOfPatrul;
    }

    public String getSimCardNumber() {
        return this.simCardNumber;
    }

    public double getLatitude() {
        return this.latitude;
    }

    public double getLongitude() {
        return this.longitude;
    }

    public double getAverageFuelConsumption() {
        return this.averageFuelConsumption;
    }

    @MethodsAnnotations(
            name = "uuid",
            withoutParams = false,
            isReturnEntity = false,
            acceptEntityType = CassandraDataTypes.UUID
    )
    public void setUuid ( @lombok.NonNull final UUID uuid ) {
        this.uuid = uuid;
    }

    @MethodsAnnotations(
            name = "uuidOfEscort",
            withoutParams = false,
            isReturnEntity = false,
            acceptEntityType = CassandraDataTypes.UUID
    )
    public void setUuidOfEscort ( final UUID uuidOfEscort ) {
        this.uuidOfEscort = uuidOfEscort;
    }

    @MethodsAnnotations(
            name = "uuidOfPatrul",
            withoutParams = false,
            isReturnEntity = false,
            acceptEntityType = CassandraDataTypes.UUID
    )
    public void setUuidOfPatrul ( final UUID uuidOfPatrul ) {
        this.uuidOfPatrul = uuidOfPatrul;
    }

    @MethodsAnnotations(
            name = "carModel",
            withoutParams = false,
            isReturnEntity = false
    )
    public void setCarModel ( final String carModel ) {
        this.carModel = carModel;
    }

    @MethodsAnnotations(
            name = "gosNumber",
            withoutParams = false,
            isReturnEntity = false
    )
    public void setGosNumber ( final String gosNumber ) {
        this.gosNumber = gosNumber;
    }

    @MethodsAnnotations(
            name = "trackerId",
            withoutParams = false,
            isReturnEntity = false
    )
    public void setTrackerId ( final String trackerId ) {
        this.trackerId = trackerId;
    }

    @MethodsAnnotations(
            name = "nsfOfPatrul",
            withoutParams = false,
            isReturnEntity = false
    )
    public void setNsfOfPatrul ( final String nsfOfPatrul ) {
        this.nsfOfPatrul = nsfOfPatrul;
    }

    @MethodsAnnotations(
            name = "simCardNumber",
            withoutParams = false,
            isReturnEntity = false
    )
    public void setSimCardNumber ( final String simCardNumber ) {
        this.simCardNumber = simCardNumber;
    }

    @MethodsAnnotations(
            name = "latitude",
            withoutParams = false,
            isReturnEntity = false,
            acceptEntityType = CassandraDataTypes.DOUBLE
    )
    public void setLatitude ( final double latitude ) {
        this.latitude = latitude;
    }

    @MethodsAnnotations(
            name = "longitude",
            withoutParams = false,
            isReturnEntity = false,
            acceptEntityType = CassandraDataTypes.DOUBLE
    )
    public void setLongitude ( final double longitude ) {
        this.longitude = longitude;
    }

    @MethodsAnnotations(
            name = "averageFuelConsumption",
            withoutParams = false,
            isReturnEntity = false,
            acceptEntityType = CassandraDataTypes.DOUBLE
    )
    public void setAverageFuelConsumption ( final double averageFuelConsumption ) {
        this.averageFuelConsumption = averageFuelConsumption;
    }

    @FieldAnnotation( name = "uuid", mightBeNull = false, cassandraType = CassandraDataTypes.UUID )
    private UUID uuid;

    @FieldAnnotation(
            name = "uuidOfEscort",
            comment = "UUID of the Escort which this car is linked to",
            mightBeNull = false,
            cassandraType = CassandraDataTypes.UUID
    )
    private UUID uuidOfEscort;

    @FieldAnnotation(
            name = "uuidOfPatrul",
            comment = "UUID of the Escort which this patrul is linked to",
            mightBeNull = false,
            cassandraType = CassandraDataTypes.UUID
    )
    private UUID uuidOfPatrul;

    @FieldAnnotation( name = "carModel", hasToBeJoinedWithAstrix = true )
    private String carModel;

    @EntityIndex( name = "gosNumber" )
    @FieldAnnotation( name = "gosNumber", hasToBeJoinedWithAstrix = true, mightBeNull = false )
    private String gosNumber;

    @EntityIndex( name = "trackerId" )
    @FieldAnnotation( name = "trackerId", hasToBeJoinedWithAstrix = true, mightBeNull = false )
    private String trackerId;

    @FieldAnnotation( name = "nsfOfPatrul", hasToBeJoinedWithAstrix = true )
    private String nsfOfPatrul;
    @FieldAnnotation( name = "simCardNumber", hasToBeJoinedWithAstrix = true )
    private String simCardNumber;

    @FieldAnnotation( name = "latitude", cassandraType = CassandraDataTypes.DOUBLE )
    private double latitude;
    @FieldAnnotation( name = "longitude", cassandraType = CassandraDataTypes.DOUBLE )
    private double longitude;
    @FieldAnnotation( name = "averageFuelConsumption", cassandraType = CassandraDataTypes.DOUBLE )
    private double averageFuelConsumption;

    private TupleOfCar() {}

    @EntityConstructorAnnotation
    public <T> TupleOfCar ( @lombok.NonNull final Class<T> instance ) {
        AnnotationInspector.checkCallerPermission( instance, TupleOfCar.class );
    }

    @Override
    @lombok.NonNull
    @lombok.Synchronized
    public synchronized Insert getEntityInsert () {
        AnnotationInspector.checkEntityFieldsNotEmpty( this );

        return this.startInsert()
                .value(
                        CqlIdentifier.fromCql( "uuid" ),
                        QueryBuilder.now()
                ).value(
                        CqlIdentifier.fromCql( "uuidOfEscort" ),
                        QueryBuilder.literal( this.getUuidOfEscort() )
                ).value(
                        CqlIdentifier.fromCql( "uuidOfPatrul" ),
                        QueryBuilder.literal( this.getUuidOfPatrul() )
                ).value(
                        CqlIdentifier.fromCql( "carModel" ),
                        QueryBuilder.literal( this.getCarModel() )
                ).value(
                        CqlIdentifier.fromCql( "gosNumber" ),
                        QueryBuilder.literal( this.getGosNumber() )
                ).value(
                        CqlIdentifier.fromCql( "trackerId" ),
                        QueryBuilder.literal( this.getTrackerId() )
                ).value(
                        CqlIdentifier.fromCql( "nsfOfPatrul" ),
                        QueryBuilder.literal( this.getNsfOfPatrul() )
                ).value(
                        CqlIdentifier.fromCql( "simCardNumber" ),
                        QueryBuilder.literal( this.getSimCardNumber() )
                ).value(
                        CqlIdentifier.fromCql( "latitude" ),
                        QueryBuilder.literal( this.getLatitude() )
                ).value(
                        CqlIdentifier.fromCql( "longitude" ),
                        QueryBuilder.literal( this.getLongitude() )
                ).value(
                        CqlIdentifier.fromCql( "averageFuelConsumption" ),
                        QueryBuilder.literal( this.getAverageFuelConsumption() )
                ).ifNotExists();
    }

    @Override
    @lombok.NonNull
    public Update getEntityUpdate () {
        return this.startUpdate().set(
                Assignment.setColumn(
                        CqlIdentifier.fromCql( "longitude" ),
                        QueryBuilder.literal( this.getLongitude() )
                ),
                Assignment.setColumn(
                        CqlIdentifier.fromCql( "latitude" ),
                        QueryBuilder.literal( this.getLatitude() )
                )
        ).where(
                Relation.column(
                        AnnotationInspector.getEntityPrimaryKey( this )[0]
                ).isEqualTo( QueryBuilder.literal( this.getUuid() ) ),
                Relation.column(
                        CqlIdentifier.fromCql( "trackerid" )
                ).isEqualTo( QueryBuilder.literal( this.getTrackerId() ) )
        );
    }

    @Override
    @lombok.NonNull
    public BatchStatement getEntityDeleteBatch () {
        return new BatchStatement().add(
                CassandraDataControl
                        .getInstance()
                        .generatePreparedStatement(
                                this.startDelete().where(
                                        Relation.column(
                                                CqlIdentifier.fromCql(
                                                        AnnotationInspector.getEntityPrimaryKey( this )[0]
                                                )
                                        ).isEqualTo( QueryBuilder.bindMarker() )
                                )
                        ).bind( QueryBuilder.literal( this.getUuid() ) )
        ).add(
                CassandraDataControl
                        .getInstance()
                        .generatePreparedStatement(
                                EntitiesInstances.TRACKER_INFO.get().startDelete().where(
                                        Relation.column(
                                                AnnotationInspector.getEntityPrimaryKey( EntitiesInstances.TRACKER_INFO.get() )[0]
                                        ).isEqualTo( QueryBuilder.bindMarker() )
                                )
                        ).bind( CqlIdentifier.fromCql( this.getTrackerId() ) )
        );
    }

    @Override
    @lombok.NonNull
    public TupleOfCar generate () {
        return new TupleOfCar();
    }

    @Override
    @lombok.NonNull
    @org.jetbrains.annotations.Contract( value = "_ -> fail" )
    public TupleOfCar generate(
            @lombok.NonNull final com.datastax.driver.core.GettableData gettableData
    ) {
        return AnnotationInspector.fillEntityParams( this, gettableData );
    }

    @Override
    @lombok.NonNull
    public String getSuccessMessage() {
        return String.join(
                StringOperations.SPACE,
                "Kafka got",
                this.getClass().getName(),
                "with id:",
                this.getTrackerId()
        );
    }
}
