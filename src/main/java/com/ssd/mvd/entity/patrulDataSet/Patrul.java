package com.ssd.mvd.entity.patrulDataSet;

import com.ssd.mvd.database.cassandraRegistry.CassandraConverter;
import com.ssd.mvd.interfaces.ObjectFromRowConvertInterface;
import com.ssd.mvd.constants.CassandraCommands;
import com.ssd.mvd.constants.CassandraTables;
import com.ssd.mvd.entity.TupleOfCar;

import com.datastax.driver.core.Row;

import java.text.MessageFormat;
import java.util.UUID;

public final class Patrul extends CassandraConverter implements ObjectFromRowConvertInterface< Patrul > {
    public UUID getUuid () {
        return this.uuid;
    }

    public void setUuid ( final UUID uuid ) {
        this.uuid = uuid;
    }

    public void setTotalActivityTime( final long totalActivityTime ) {
        this.totalActivityTime = totalActivityTime;
    }

    public void setInPolygon( final boolean inPolygon ) {
        this.inPolygon = inPolygon;
    }

    public void setTuplePermission( final boolean tuplePermission ) {
        this.tuplePermission = tuplePermission;
    }

    public void setRank( final String rank ) {
        this.rank = rank;
    }

    public void setEmail( final String email ) {
        this.email = email;
    }

    public void setOrganName( final String organName ) {
        this.organName = organName;
    }

    public String getPoliceType() {
        return this.policeType;
    }

    public void setPoliceType( final String policeType ) {
        this.policeType = policeType;
    }

    public void setDateOfBirth( final String dateOfBirth ) {
        this.dateOfBirth = dateOfBirth;
    }

    public String getPassportNumber() {
        return this.passportNumber;
    }

    public void setPassportNumber( final String passportNumber ) {
        this.passportNumber = passportNumber;
    }

    public void setPatrulImageLink( final String patrulImageLink ) {
        this.patrulImageLink = patrulImageLink;
    }

    public PatrulFIOData getPatrulFIOData() {
        return this.patrulFIOData;
    }

    public void setPatrulFIOData( final PatrulFIOData patrulFIOData ) {
        this.patrulFIOData = patrulFIOData;
    }

    public PatrulCarInfo getPatrulCarInfo() {
        return this.patrulCarInfo;
    }

    public void setPatrulCarInfo( final PatrulCarInfo patrulCarInfo ) {
        this.patrulCarInfo = patrulCarInfo;
    }

    public PatrulTaskInfo getPatrulTaskInfo() {
        return this.patrulTaskInfo;
    }

    public void setPatrulTaskInfo( final PatrulTaskInfo patrulTaskInfo ) {
        this.patrulTaskInfo = patrulTaskInfo;
    }

    public PatrulRegionData getPatrulRegionData() {
        return this.patrulRegionData;
    }

    public void setPatrulRegionData( final PatrulRegionData patrulRegionData ) {
        this.patrulRegionData = patrulRegionData;
    }

    public PatrulLocationData getPatrulLocationData() {
        return this.patrulLocationData;
    }

    public void setPatrulLocationData( final PatrulLocationData patrulLocationData ) {
        this.patrulLocationData = patrulLocationData;
    }

    public PatrulUniqueValues getPatrulUniqueValues() {
        return this.patrulUniqueValues;
    }

    public void setPatrulUniqueValues( final PatrulUniqueValues patrulUniqueValues ) {
        this.patrulUniqueValues = patrulUniqueValues;
    }

    // уникальное ID патрульного
    private UUID uuid;

    private long totalActivityTime;

    private boolean inPolygon;
    private boolean tuplePermission; // показывает можно ли патрульному участвовать в кортеже

    private String rank;
    private String email;
    private String organName;
    private String policeType; // choosing from dictionary
    private String dateOfBirth;
    private String passportNumber;
    private String patrulImageLink;

    private PatrulFIOData patrulFIOData;
    private PatrulCarInfo patrulCarInfo;
    private PatrulTaskInfo patrulTaskInfo;
    private PatrulRegionData patrulRegionData;
    private PatrulLocationData patrulLocationData;
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
