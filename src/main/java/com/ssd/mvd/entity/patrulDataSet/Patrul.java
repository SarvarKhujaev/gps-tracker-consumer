package com.ssd.mvd.entity.patrulDataSet;

import com.ssd.mvd.annotations.entity.object.EntityAnnotations;
import com.ssd.mvd.annotations.entity.object.ClusteringOrder;

import com.ssd.mvd.annotations.entity.method.MethodsAnnotations;
import com.ssd.mvd.annotations.entity.field.FieldAnnotation;

import com.ssd.mvd.annotations.entity.object.EntityConstructorAnnotation;
import com.ssd.mvd.interfaces.entity.ObjectFromRowConvertInterface;

import com.ssd.mvd.inspectors.dataTypesInpectors.StringOperations;
import com.ssd.mvd.inspectors.AnnotationInspector;

import com.ssd.mvd.entity.patrulDataSet.patrulSubClasses.*;
import com.ssd.mvd.entity.TupleOfCar;

import com.ssd.mvd.constants.cassandra.CassandraDataTypes;
import com.ssd.mvd.constants.cassandra.CassandraCommands;
import com.ssd.mvd.constants.cassandra.CassandraTables;

import java.text.MessageFormat;
import java.util.UUID;

@EntityAnnotations(
        name = "Patrul",
        comment = "хранит данные, обо всех патрульных, является основной таблицей",
        tableName = CassandraTables.PATRULS,
        primaryKeys = {
                "uuid",
                "passportNumber"
        },
        clusteringKeys = {
                @ClusteringOrder( columnName = "passportNumber" )
        }
)
public final class Patrul extends AnnotationInspector implements ObjectFromRowConvertInterface< Patrul > {
    @MethodsAnnotations(
            name = "uuid",
            isPrimaryKey = true
    )
    public UUID getUuid () {
        return this.uuid;
    }

    @MethodsAnnotations(
            name = "uuid",
            withoutParams = false,
            isReturnEntity = false,
            acceptEntityType = CassandraDataTypes.UUID
    )
    public void setUuid ( final UUID uuid ) {
        this.uuid = uuid;
    }

    @MethodsAnnotations(
            name = "totalActivityTime",
            isReturnEntity = false,
            withoutParams = false,
            acceptEntityType = CassandraDataTypes.BIGINT
    )
    public void setTotalActivityTime( final long totalActivityTime ) {
        this.totalActivityTime = totalActivityTime;
    }

    @MethodsAnnotations(
            name = "inPolygon",
            withoutParams = false,
            isReturnEntity = false,
            acceptEntityType = CassandraDataTypes.BOOLEAN
    )
    public void setInPolygon( final boolean inPolygon ) {
        this.inPolygon = inPolygon;
    }

    @MethodsAnnotations(
            name = "tuplePermission",
            withoutParams = false,
            isReturnEntity = false,
            acceptEntityType = CassandraDataTypes.BOOLEAN
    )
    public void setTuplePermission( final boolean tuplePermission ) {
        this.tuplePermission = tuplePermission;
    }

    @MethodsAnnotations(
            name = "rank",
            withoutParams = false,
            isReturnEntity = false
    )
    public void setRank( final String rank ) {
        this.rank = rank;
    }

    @MethodsAnnotations(
            name = "email",
            withoutParams = false,
            isReturnEntity = false
    )
    public void setEmail( final String email ) {
        this.email = email;
    }

    @MethodsAnnotations(
            name = "organName",
            withoutParams = false,
            isReturnEntity = false
    )
    public void setOrganName( final String organName ) {
        this.organName = organName;
    }

    @MethodsAnnotations(
            name = "policeType",
            withoutParams = false,
            isReturnEntity = false
    )
    public void setPoliceType( final String policeType ) {
        this.policeType = policeType;
    }

    @MethodsAnnotations(
            name = "dateOfBirth",
            isReturnEntity = false,
            withoutParams = false
    )
    public void setDateOfBirth( final String dateOfBirth ) {
        this.dateOfBirth = dateOfBirth;
    }

    @MethodsAnnotations(
            name = "passportNumber",
            withoutParams = false,
            isReturnEntity = false
    )
    public void setPassportNumber( final String passportNumber ) {
        this.passportNumber = passportNumber;
    }

    @MethodsAnnotations(
            name = "patrulImageLink",
            withoutParams = false,
            isReturnEntity = false
    )
    public void setPatrulImageLink( final String patrulImageLink ) {
        this.patrulImageLink = patrulImageLink;
    }

    public void setPatrulFIOData( final PatrulFIOData patrulFIOData ) {
        this.patrulFIOData = patrulFIOData;
    }

    public void setPatrulCarInfo( final PatrulCarInfo patrulCarInfo ) {
        this.patrulCarInfo = patrulCarInfo;
    }

    public void setPatrulTaskInfo( final PatrulTaskInfo patrulTaskInfo ) {
        this.patrulTaskInfo = patrulTaskInfo;
    }

    public void setPatrulRegionData( final PatrulRegionData patrulRegionData ) {
        this.patrulRegionData = patrulRegionData;
    }

    public void setPatrulLocationData( final PatrulLocationData patrulLocationData ) {
        this.patrulLocationData = patrulLocationData;
    }

    public void setPatrulUniqueValues( final PatrulUniqueValues patrulUniqueValues ) {
        this.patrulUniqueValues = patrulUniqueValues;
    }

    public String getPoliceType() {
        return this.policeType;
    }

    public String getPassportNumber() {
        return this.passportNumber;
    }

    public PatrulFIOData getPatrulFIOData() {
        return this.patrulFIOData;
    }

    public PatrulCarInfo getPatrulCarInfo() {
        return this.patrulCarInfo;
    }

    public PatrulTaskInfo getPatrulTaskInfo() {
        return this.patrulTaskInfo;
    }

    public PatrulRegionData getPatrulRegionData() {
        return this.patrulRegionData;
    }

    public PatrulLocationData getPatrulLocationData() {
        return this.patrulLocationData;
    }

    public PatrulUniqueValues getPatrulUniqueValues() {
        return this.patrulUniqueValues;
    }

    @FieldAnnotation( name = "uuid", mightBeNull = false )
    private UUID uuid;

    @FieldAnnotation( name = "totalActivityTime" )
    private long totalActivityTime;

    @FieldAnnotation( name = "inPolygon" )
    private boolean inPolygon;
    @FieldAnnotation( name = "tuplePermission" )
    private boolean tuplePermission;

    @FieldAnnotation( name = "rank", hasToBeJoinedWithAstrix = true )
    private String rank;
    @FieldAnnotation( name = "email", hasToBeJoinedWithAstrix = true, mightBeNull = false )
    private String email;
    @FieldAnnotation( name = "organName", hasToBeJoinedWithAstrix = true )
    private String organName;
    @FieldAnnotation( name = "policeType", hasToBeJoinedWithAstrix = true, mightBeNull = false )
    private String policeType;
    @FieldAnnotation( name = "dateOfBirth", hasToBeJoinedWithAstrix = true, mightBeNull = false )
    private String dateOfBirth;
    @FieldAnnotation( name = "passportNumber", hasToBeJoinedWithAstrix = true, mightBeNull = false )
    private String passportNumber;
    @FieldAnnotation( name = "patrulImageLink", hasToBeJoinedWithAstrix = true )
    private String patrulImageLink;

    @FieldAnnotation( name = "patrulFIOData", mightBeNull = false, isInteriorObject = true )
    private PatrulFIOData patrulFIOData;
    @FieldAnnotation( name = "patrulCarInfo", mightBeNull = false, isInteriorObject = true )
    private PatrulCarInfo patrulCarInfo;
    @FieldAnnotation( name = "patrulTaskInfo", mightBeNull = false, isInteriorObject = true )
    private PatrulTaskInfo patrulTaskInfo;
    @FieldAnnotation( name = "patrulRegionData", mightBeNull = false, isInteriorObject = true )
    private PatrulRegionData patrulRegionData;
    @FieldAnnotation( name = "patrulLocationData", mightBeNull = false, isInteriorObject = true )
    private PatrulLocationData patrulLocationData;
    @FieldAnnotation( name = "patrulUniqueValues", mightBeNull = false, isInteriorObject = true )
    private PatrulUniqueValues patrulUniqueValues;

    public void linkWithTupleOfCar ( final TupleOfCar tupleOfCar ) {
        this.getPatrulUniqueValues().setUuidForEscortCar( tupleOfCar.getUuid() );
        this.getPatrulCarInfo().setCarNumber( tupleOfCar.getGosNumber() );
    }

    private Patrul () {}

    @EntityConstructorAnnotation
    public <T> Patrul ( final Class<T> instance ) {
        AnnotationInspector.checkCallerPermission( instance, Patrul.class );
    }

    @Override
    @lombok.NonNull
    public Patrul generate() {
        return new Patrul();
    }

    @Override
    @lombok.NonNull
    @org.jetbrains.annotations.Contract( value = "_ -> fail" )
    public synchronized Patrul generate( final com.datastax.driver.core.GettableData gettableData ) {
        checkAndSetParams(
                gettableData,
                udtValue1 -> {
                    this.setPatrulFIOData(
                            this.getPatrulFIOData()
                                    .generate()
                                    .generate( gettableData.getUDTValue( getSubClassColumnName( this.getPatrulFIOData() ) ) )
                    );
                    this.setPatrulCarInfo(
                            this.getPatrulCarInfo()
                                    .generate()
                                    .generate( gettableData.getUDTValue( getSubClassColumnName( this.getPatrulCarInfo() ) ) )
                    );
                    this.setPatrulTaskInfo(
                            this.getPatrulTaskInfo()
                                    .generate()
                                    .generate( gettableData.getUDTValue( getSubClassColumnName( this.getPatrulTaskInfo() ) ) )
                    );
                    this.setPatrulRegionData(
                            this.getPatrulRegionData()
                                    .generate()
                                    .generate( gettableData.getUDTValue( getSubClassColumnName( this.getPatrulRegionData() ) ) )
                    );
                    this.setPatrulLocationData(
                            this.getPatrulLocationData()
                                    .generate()
                                    .generate( gettableData.getUDTValue( getSubClassColumnName( this.getPatrulLocationData() ) ) )
                    );
                    this.setPatrulUniqueValues(
                            this.getPatrulUniqueValues()
                                    .generate()
                                    .generate( gettableData.getUDTValue( getSubClassColumnName( this.getPatrulUniqueValues() ) ) )
                    );
                }
        );

        return fillEntityParams( this, gettableData );
    }

    @Override
    @lombok.NonNull
    public String getEntityUpdateCommand() {
        return MessageFormat.format(
                """
                {0} {1}.{2}
                SET uuidForEscortCar = {3},
                carType = {4},
                carNumber = {5},
                WHERE uuid = {6};
                """,
                CassandraCommands.UPDATE,

                this.getEntityKeyspaceName(),
                this.getEntityTableName(),

                this.getPatrulUniqueValues().getUuidForEscortCar(),
                StringOperations.joinWithAstrix( this.getPatrulCarInfo().getCarType() ),
                StringOperations.joinWithAstrix( this.getPatrulCarInfo().getCarNumber() ),
                this.getUuid()
        );
    }
}
