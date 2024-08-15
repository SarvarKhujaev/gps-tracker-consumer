package com.ssd.mvd.entity.patrulDataSet;

import com.ssd.mvd.database.cassandraRegistry.CassandraConverter;
import com.ssd.mvd.interfaces.ObjectFromRowConvertInterface;
import com.ssd.mvd.entity.patrulDataSet.patrulSubClasses.*;
import com.ssd.mvd.constants.CassandraDataTypes;
import com.ssd.mvd.constants.CassandraCommands;
import com.ssd.mvd.constants.CassandraTables;
import com.ssd.mvd.entity.TupleOfCar;
import com.ssd.mvd.annotations.*;

import com.datastax.driver.core.Row;

import java.text.MessageFormat;
import java.util.UUID;

@EntityAnnotations( name = "Patrul" )
public final class Patrul extends CassandraConverter implements ObjectFromRowConvertInterface< Patrul > {
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

    // уникальное ID патрульного
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

    /*
        соединяем патрульного с Эскортом
    */
    public void linkWithTupleOfCar ( final TupleOfCar tupleOfCar ) {
        this.getPatrulUniqueValues().setUuidForEscortCar( tupleOfCar.getUuid() );
        this.getPatrulCarInfo().setCarNumber( tupleOfCar );
    }

    public Patrul () {}

    @Override
    public CassandraTables getEntityTableName () {
        return CassandraTables.PATRULS;
    }

    @Override
    public CassandraTables getEntityKeyspaceName () {
        return CassandraTables.TABLETS;
    }

    @Override
    public Patrul generate() {
        return new Patrul();
    }

    @Override
    public Patrul generate( final Row row ) {
        super.checkAndSetParams(
                row,
                row1 -> {
                    this.setUuid( row.getUUID( "uuid" ) );
                    this.setInPolygon( row.getBool( "inPolygon" ) );
                    this.setTuplePermission( row.getBool( "tuplePermission" ) );
                    this.setTotalActivityTime( row.getLong( "totalActivityTime" ) );

                    this.setRank( row.getString( "rank" ) );
                    this.setEmail( row.getString( "email" ) );
                    this.setOrganName( row.getString( "organName" ) );
                    this.setPoliceType( row.getString( "policeType" ) );
                    this.setDateOfBirth( row.getString( "dateOfBirth" ) );
                    this.setPassportNumber( row.getString( "passportNumber" ) );
                    this.setPatrulImageLink( row.getString( "patrulImageLink" ) );

                    this.setPatrulCarInfo( new PatrulCarInfo().generate( row.getUDTValue( "patrulCarInfo" ) ) );
                    this.setPatrulFIOData( new PatrulFIOData().generate( row.getUDTValue( "patrulFIOData" ) ) );
                    this.setPatrulTaskInfo( new PatrulTaskInfo().generate( row.getUDTValue( "patrulTaskInfo" ) ) );
                    this.setPatrulRegionData( new PatrulRegionData().generate( row.getUDTValue( "patrulRegionData" ) ) );
                    this.setPatrulLocationData( new PatrulLocationData().generate( row.getUDTValue( "patrulLocationData" ) ) );
                    this.setPatrulUniqueValues( new PatrulUniqueValues().generate( row.getUDTValue( "patrulUniqueValues" ) ) );
                }
        );

        return this;
    }

    @Override
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
                super.joinWithAstrix( this.getPatrulCarInfo().getCarType() ),
                super.joinWithAstrix( this.getPatrulCarInfo().getCarNumber() ),
                this.getUuid()
        );
    }
}
