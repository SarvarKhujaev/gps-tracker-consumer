package com.ssd.mvd.entity;

import com.ssd.mvd.database.cassandraRegistry.CassandraConverter;
import com.ssd.mvd.interfaces.ObjectFromRowConvertInterface;
import com.ssd.mvd.interfaces.KafkaEntitiesCommonMethods;
import com.ssd.mvd.kafka.kafkaConfigs.KafkaTopics;
import com.ssd.mvd.constants.CassandraFunctions;
import com.ssd.mvd.constants.CassandraCommands;
import com.ssd.mvd.constants.CassandraTables;

import com.google.gson.annotations.Expose;
import com.datastax.driver.core.Row;

import java.text.MessageFormat;
import java.util.UUID;

public final class TupleOfCar
        extends CassandraConverter
        implements ObjectFromRowConvertInterface< TupleOfCar >, KafkaEntitiesCommonMethods {
    public UUID getUuid() {
        return this.uuid;
    }

    public String getCarModel() {
        return this.carModel;
    }

    public double getLatitude() {
        return this.latitude;
    }

    public double getLongitude() {
        return this.longitude;
    }

    public String getGosNumber() {
        return this.gosNumber;
    }

    public String getTrackerId() {
        return this.trackerId;
    }

    public UUID getUuidOfEscort() {
        return this.uuidOfEscort;
    }

    public UUID getUuidOfPatrul() {
        return this.uuidOfPatrul;
    }

    public String getNsfOfPatrul() {
        return this.nsfOfPatrul;
    }

    public String getSimCardNumber() {
        return this.simCardNumber;
    }

    public double getAverageFuelConsumption() {
        return this.averageFuelConsumption;
    }

    public void setUuid ( final UUID uuid ) {
        this.uuid = uuid;
    }

    public void setCarModel ( final String carModel ) {
        this.carModel = carModel;
    }

    public void setLatitude ( final double latitude ) {
        this.latitude = latitude;
    }

    public void setLongitude ( final double longitude ) {
        this.longitude = longitude;
    }

    public void setGosNumber ( final String gosNumber ) {
        this.gosNumber = gosNumber;
    }

    public void setTrackerId ( final String trackerId ) {
        this.trackerId = trackerId;
    }

    public void setNsfOfPatrul ( final String nsfOfPatrul ) {
        this.nsfOfPatrul = nsfOfPatrul;
    }

    public void setUuidOfEscort ( final UUID uuidOfEscort ) {
        this.uuidOfEscort = uuidOfEscort;
    }

    public void setUuidOfPatrul ( final UUID uuidOfPatrul ) {
        this.uuidOfPatrul = uuidOfPatrul;
    }

    public void setSimCardNumber ( final String simCardNumber ) {
        this.simCardNumber = simCardNumber;
    }

    public void setAverageFuelConsumption ( final double averageFuelConsumption ) {
        this.averageFuelConsumption = averageFuelConsumption;
    }

    @Expose
    private UUID uuid;
    @Expose
    private UUID uuidOfEscort; // UUID of the Escort which this car is linked to
    @Expose
    private UUID uuidOfPatrul;

    @Expose
    private String carModel;
    @Expose
    private String gosNumber;
    @Expose
    private String trackerId;
    @Expose
    private String nsfOfPatrul;
    @Expose
    private String simCardNumber;

    @Expose
    private double latitude;
    @Expose
    private double longitude;
    @Expose
    private double averageFuelConsumption;

    public TupleOfCar () {}

    @Override
    public String getEntityUpdateCommand () {
        return MessageFormat.format(
                """
                {0} {1}.{2}
                SET longitude = {3}, latitude = {4}
                WHERE uuid = {5} AND trackerid = {6};
                """,
                CassandraCommands.UPDATE,

                this.getEntityKeyspaceName(),
                this.getEntityTableName(),

                this.getLongitude(),
                this.getLatitude(),
                this.getUuid(),
                super.joinWithAstrix( this.getTrackerId() )
        );
    }

    @Override
    public String getEntityInsertCommand () {
        return MessageFormat.format(
                """
                {0} {1}.{2} {3}
                VALUES ( {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14} );
                """,
                CassandraCommands.INSERT_INTO,

                this.getEntityKeyspaceName(),
                this.getEntityTableName(),

                super.getALlParamsNamesForClass.apply( this.getClass() ),

                CassandraFunctions.UUID,

                this.getUuidOfEscort(),
                this.getUuidOfPatrul(),

                super.joinWithAstrix( this.getCarModel() ),
                super.joinWithAstrix( this.getGosNumber() ),
                super.joinWithAstrix( this.getTrackerId() ),
                super.joinWithAstrix( this.getNsfOfPatrul() ),
                super.joinWithAstrix( this.getSimCardNumber() ),

                this.getLatitude(),
                this.getLongitude(),
                this.getAverageFuelConsumption()
        );
    }

    @Override
    public String getEntityDeleteCommand () {
        return MessageFormat.format(
                """
                {0} {1} {2} {3}
                """,
                CassandraCommands.BEGIN_BATCH,

                MessageFormat.format(
                        """
                        {0} {1}.{2} WHERE uuid = {3};
                        """,
                        CassandraCommands.DELETE,

                        this.getEntityKeyspaceName(),
                        this.getEntityTableName(),

                        this.getUuid()
                ),

                MessageFormat.format(
                        """
                        {0} {1}.{2} WHERE trackersId = {3} {4};
                        """,
                        CassandraCommands.DELETE,

                        this.getEntityKeyspaceName(),
                        CassandraTables.TRACKERSID,

                        this.getTrackerId(),

                        CassandraCommands.IF_EXISTS
                ),

                CassandraCommands.APPLY_BATCH
        );
    }

    @Override
    public CassandraTables getEntityTableName() {
        return CassandraTables.TUPLE_OF_CAR;
    }

    @Override
    public CassandraTables getEntityKeyspaceName() {
        return CassandraTables.ESCORT;
    }

    @Override
    public TupleOfCar generate( final Row row ) {
        super.checkAndSetParams(
                row,
                row1 -> {
                    this.setUuid( row.getUUID( "uuid" ) );
                    this.setUuidOfEscort( row.getUUID( "uuidOfEscort" ) );
                    this.setUuidOfPatrul( row.getUUID( "uuidOfPatrul" ) );

                    this.setCarModel( row.getString( "carModel" ) );
                    this.setGosNumber( row.getString( "gosNumber" ) );
                    this.setTrackerId( row.getString( "trackerId" ) );
                    this.setNsfOfPatrul( row.getString( "nsfOfPatrul" ) );
                    this.setSimCardNumber( row.getString( "simCardNumber" ) );

                    this.setLatitude( row.getDouble( "latitude" ) );
                    this.setLongitude( row.getDouble( "longitude" ) );
                    this.setAverageFuelConsumption( row.getDouble( "averageFuelConsumption" ) );
                }
        );

        return this;
    }

    @Override
    public TupleOfCar generate() {
        return new TupleOfCar();
    }

    @Override
    public KafkaTopics getTopicName() {
        return KafkaTopics.NEW_TUPLE_OF_CAR_TOPIC;
    }

    @Override
    public String getSuccessMessage() {
        return String.join(
                " ",
                "Kafka got",
                this.getClass().getName(),
                "with id:",
                this.getTrackerId()
        );
    }
}
