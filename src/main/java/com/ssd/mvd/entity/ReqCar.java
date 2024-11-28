package com.ssd.mvd.entity;

import com.datastax.driver.core.BatchStatement;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;
import com.ssd.mvd.annotations.entity.object.EntityConstructorAnnotation;
import com.ssd.mvd.annotations.entity.object.EntityAnnotations;

import com.ssd.mvd.annotations.entity.field.ChildEntityAnnotation;
import com.ssd.mvd.annotations.entity.field.FieldAnnotation;
import com.ssd.mvd.annotations.entity.field.EntityIndex;

import com.ssd.mvd.annotations.entity.method.MethodsAnnotations;
import com.ssd.mvd.annotations.kafka.KafkaEntityAnnotation;

import com.ssd.mvd.database.CassandraDataControl;
import com.ssd.mvd.inspectors.EntitiesInstances;
import com.ssd.mvd.interfaces.entity.ObjectFromRowConvertInterface;
import com.ssd.mvd.interfaces.KafkaEntitiesCommonMethods;

import com.ssd.mvd.inspectors.dataTypesInpectors.StringOperations;
import com.ssd.mvd.inspectors.AnnotationInspector;
import com.ssd.mvd.inspectors.CassandraConverter;

import com.ssd.mvd.kafka.kafkaConfigs.KafkaTopics;
import com.ssd.mvd.entity.patrulDataSet.Patrul;

import com.ssd.mvd.constants.cassandra.CassandraDataTypes;
import com.ssd.mvd.constants.cassandra.CassandraFunctions;
import com.ssd.mvd.constants.cassandra.CassandraCommands;
import com.ssd.mvd.constants.cassandra.CassandraTables;

import java.text.MessageFormat;
import java.util.UUID;

@EntityAnnotations(
        name = "ReqCar",
        comment = "Данные о патрульных машинах",
        tableName = CassandraTables.CARS
)
@KafkaEntityAnnotation( topicName = KafkaTopics.NEW_CAR_TOPIC )
public final class ReqCar
        extends CassandraConverter
        implements ObjectFromRowConvertInterface< ReqCar >, KafkaEntitiesCommonMethods {
    @MethodsAnnotations(
            name = "uuid",
            isPrimaryKey = true
    )
    @lombok.NonNull
    public UUID getUuid() {
        return this.uuid;
    }

    @lombok.NonNull
    public UUID getPatrulId() {
        return this.patrulId;
    }

    @lombok.NonNull
    public UUID getLustraId() {
        return this.lustraId;
    }

    public int getSideNumber() {
        return this.sideNumber;
    }

    public double getLatitude() {
        return this.latitude;
    }

    @lombok.NonNull
    public String getGosNumber() {
        return this.gosNumber;
    }

    @lombok.NonNull
    public String getTrackerId() {
        return this.trackerId;
    }

    public double getLongitude() {
        return this.longitude;
    }

    public int getSimCardNumber() {
        return this.simCardNumber;
    }

    public String getVehicleType() {
        return this.vehicleType;
    }

    public String getCarImageLink() {
        return this.carImageLink;
    }

    public double getAverageFuelSize() {
        return this.averageFuelSize;
    }

    public String getPatrulPassportSeries() {
        return this.patrulPassportSeries;
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
            name = "patrulId",
            withoutParams = false,
            isReturnEntity = false,
            acceptEntityType = CassandraDataTypes.UUID
    )
    public void setPatrulId( @lombok.NonNull final UUID uuid ) {
        this.patrulId = uuid;
    }

    @MethodsAnnotations(
            name = "lustraId",
            withoutParams = false,
            isReturnEntity = false,
            acceptEntityType = CassandraDataTypes.UUID
    )
    public void setLustraId ( final UUID lustraId ) {
        this.lustraId = lustraId;
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
            name = "sideNumber",
            withoutParams = false,
            isReturnEntity = false,
            acceptEntityType = CassandraDataTypes.INT
    )
    public void setSideNumber ( final int sideNumber ) {
        this.sideNumber = sideNumber;
    }

    @MethodsAnnotations(
            name = "simCardNumber",
            withoutParams = false,
            isReturnEntity = false,
            acceptEntityType = CassandraDataTypes.INT
    )
    public void setSimCardNumber ( final int simCardNumber ) {
        this.simCardNumber = simCardNumber;
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
            name = "vehicleType",
            withoutParams = false,
            isReturnEntity = false
    )
    public void setVehicleType ( final String vehicleType ) {
        this.vehicleType = vehicleType;
    }

    @MethodsAnnotations(
            name = "carImageLink",
            withoutParams = false,
            isReturnEntity = false
    )
    public void setCarImageLink ( final String carImageLink ) {
        this.carImageLink = carImageLink;
    }

    @MethodsAnnotations(
            name = "patrulPassportSeries",
            withoutParams = false,
            isReturnEntity = false
    )
    public void setPatrulPassportSeries ( final String patrulPassportSeries ) {
        this.patrulPassportSeries = patrulPassportSeries;
    }

    @MethodsAnnotations(
            name = "averageFuelSize",
            withoutParams = false,
            isReturnEntity = false,
            acceptEntityType = CassandraDataTypes.DOUBLE
    )
    public void setAverageFuelSize ( final double averageFuelSize ) {
        this.averageFuelSize = averageFuelSize;
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

    @FieldAnnotation( name = "uuid", mightBeNull = false )
    private UUID uuid;
    @FieldAnnotation( name = "lustraId", mightBeNull = false )
    private UUID lustraId;
    @FieldAnnotation( name = "patrulId", mightBeNull = false )
    private UUID patrulId;

    @FieldAnnotation( name = "gosNumber", hasToBeJoinedWithAstrix = true, mightBeNull = false )
    private String gosNumber;
    @EntityIndex( name = "trackerId" )
    @FieldAnnotation( name = "trackerId", hasToBeJoinedWithAstrix = true, mightBeNull = false )
    private String trackerId;
    @FieldAnnotation( name = "vehicleType", hasToBeJoinedWithAstrix = true, mightBeNull = false )
    private String vehicleType;
    @FieldAnnotation( name = "carImageLink", hasToBeJoinedWithAstrix = true )
    private String carImageLink;
    @FieldAnnotation( name = "patrulPassportSeries", hasToBeJoinedWithAstrix = true, mightBeNull = false )
    @ChildEntityAnnotation(
            joinedTable = Patrul.class,
            joiningColumns = { "passportNumber" }
    )
    private String patrulPassportSeries;

    @FieldAnnotation( name = "sideNumber", comment = "бортовой номер" )
    private int sideNumber;
    @FieldAnnotation( name = "simCardNumber" )
    private int simCardNumber;

    @FieldAnnotation( name = "latitude" )
    private double latitude;
    @FieldAnnotation( name = "longitude" )
    private double longitude;
    @FieldAnnotation( name = "averageFuelSize", comment = "средний расход топлива по документам" )
    private double averageFuelSize;
    @FieldAnnotation( name = "averageFuelConsumption", comment = "средний расход топлива исходя из стиля вождения водителя" )
    private double averageFuelConsumption = 0.0;

    private ReqCar () {}

    @EntityConstructorAnnotation
    public <T> ReqCar ( final Class<T> instance ) {
        checkCallerPermission( instance, ReqCar.class );
    }

    @lombok.NonNull
    @lombok.Synchronized
    public synchronized BatchStatement getEntityInsertBatch () {
        return new BatchStatement().add(
                CassandraDataControl
                        .getInstance()
                        .generatePreparedStatement(
                                QueryBuilder.update(
                                        this.getEntityKeyspaceName().name(),
                                        EntitiesInstances.PATRUL.get().getEntityTableName().name()
                                ).set(
                                        Assignment.setField(
                                                CqlIdentifier.fromCql(
                                                        AnnotationInspector.getSubClassColumnName(
                                                                EntitiesInstances.PATRUL_CAR_INFO.get()
                                                        )
                                                ),
                                                CqlIdentifier.fromCql( "carNumber" ),
                                                QueryBuilder.bindMarker()
                                        ),
                                        Assignment.setField(
                                                CqlIdentifier.fromCql(
                                                        AnnotationInspector.getSubClassColumnName(
                                                                EntitiesInstances.PATRUL_CAR_INFO.get()
                                                        )
                                                ),
                                                CqlIdentifier.fromCql( "carType" ),
                                                QueryBuilder.bindMarker()
                                        ),
                                        Assignment.setField(
                                                CqlIdentifier.fromCql(
                                                        AnnotationInspector.getSubClassColumnName(
                                                                EntitiesInstances.PATRUL_UNIQUE_VALUES.get()
                                                        )
                                                ),
                                                CqlIdentifier.fromCql( "uuidForPatrulCar" ),
                                                QueryBuilder.bindMarker()
                                        )
                                ).where(
                                        Relation.column(
                                                CqlIdentifier.fromCql( "uuid" )
                                        ).isEqualTo( QueryBuilder.bindMarker() )
                                )
                        ).bind(
                                QueryBuilder.literal( this.getGosNumber() ),
                                QueryBuilder.literal( this.getVehicleType() ),
                                QueryBuilder.literal( this.getUuid() ),
                                QueryBuilder.literal( this.getPatrulId() )
                        )
        ).add(
                CassandraDataControl
                        .getInstance()
                        .generatePreparedStatement(
                                this.startInsert()
                                        .value(
                                                CqlIdentifier.fromCql( "uuid" ),
                                                QueryBuilder.now()
                                        )
                                        .value(
                                                CqlIdentifier.fromCql( "lustraId" ),
                                                QueryBuilder.bindMarker()
                                        )
                                        .value(
                                                CqlIdentifier.fromCql( "patrulId" ),
                                                QueryBuilder.bindMarker()
                                        )
                                        .value(
                                                CqlIdentifier.fromCql( "gosNumber" ),
                                                QueryBuilder.bindMarker()
                                        )
                                        .value(
                                                CqlIdentifier.fromCql( "trackerId" ),
                                                QueryBuilder.bindMarker()
                                        )
                                        .value(
                                                CqlIdentifier.fromCql( "vehicleType" ),
                                                QueryBuilder.bindMarker()
                                        )
                                        .value(
                                                CqlIdentifier.fromCql( "carImageLink" ),
                                                QueryBuilder.bindMarker()
                                        )
                                        .value(
                                                CqlIdentifier.fromCql( "patrulPassportSeries" ),
                                                QueryBuilder.bindMarker()
                                        )
                                        .value(
                                                CqlIdentifier.fromCql( "sideNumber" ),
                                                QueryBuilder.bindMarker()
                                        )
                                        .value(
                                                CqlIdentifier.fromCql( "simCardNumber" ),
                                                QueryBuilder.bindMarker()
                                        )
                                        .value(
                                                CqlIdentifier.fromCql( "latitude" ),
                                                QueryBuilder.bindMarker()
                                        )
                                        .value(
                                                CqlIdentifier.fromCql( "longitude" ),
                                                QueryBuilder.bindMarker()
                                        )
                        ).bind(
                                QueryBuilder.literal( this.getLatitude() ),
                                QueryBuilder.literal( this.getPatrulId() ),
                                QueryBuilder.literal( this.getGosNumber() ),
                                QueryBuilder.literal( this.getTrackerId() ),
                                QueryBuilder.literal( this.getVehicleType() ),
                                QueryBuilder.literal( this.getCarImageLink() ),
                                QueryBuilder.literal( this.getPatrulPassportSeries() ),
                                QueryBuilder.literal( this.getSideNumber() ),
                                QueryBuilder.literal( this.getSimCardNumber() ),
                                QueryBuilder.literal( this.getLatitude() ),
                                QueryBuilder.literal( this.getLongitude() )
                        )
        );
    }

    @Override
    @lombok.NonNull
    public String getEntityDeleteCommand() {
        return MessageFormat.format(
                """
                {0} {1} {2} {3};
                """,
                CassandraCommands.BEGIN_BATCH,

                MessageFormat.format(
                        """
                        {0} {1}.{2} WHERE uuid = {3};
                        """,
                        CassandraCommands.DELETE,

                        this.getEntityKeyspaceName(),
                        this.getEntityTableName(),

                        this.getGosNumber()
                ),

                MessageFormat.format(
                        """
                        {0} {1}.{2} WHERE trackersId = {3};
                        """,
                        CassandraCommands.DELETE,

                        CassandraTables.TRACKERS,
                        CassandraTables.TRACKERSID,

                        StringOperations.joinWithAstrix( this.getTrackerId() )
                ),

                CassandraCommands.APPLY_BATCH
        );
    }

    @Override
    @lombok.NonNull
    public String getEntityUpdateCommand() {
        return MessageFormat.format(
                """
                {0} {1}.{2}
                SET longitude = {3}, latitude = {4}
                WHERE uuid = {5};
                """,
                CassandraCommands.UPDATE,

                this.getEntityKeyspaceName(),
                this.getEntityTableName(),

                this.getLongitude(),
                this.getLatitude(),
                this.getUuid()
        );
    }

    @Override
    @lombok.NonNull
    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_ -> fail" )
    public synchronized ReqCar generate( @lombok.NonNull final com.datastax.driver.core.GettableData gettableData ) {
        return AnnotationInspector.fillEntityParams( this, gettableData );
    }

    @Override
    @lombok.NonNull
    public String getSuccessMessage() {
        return "Kafka got ReqCar: " + this.getTrackerId();
    }
}
