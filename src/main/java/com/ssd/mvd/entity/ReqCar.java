package com.ssd.mvd.entity;

import com.datastax.driver.core.Row;
import com.ssd.mvd.constants.CassandraCommands;
import com.ssd.mvd.constants.CassandraFunctions;
import com.ssd.mvd.constants.CassandraTables;
import com.ssd.mvd.database.cassandraRegistry.CassandraConverter;
import com.ssd.mvd.interfaces.KafkaEntitiesCommonMethods;
import com.ssd.mvd.interfaces.ObjectFromRowConvertInterface;
import com.ssd.mvd.kafka.kafkaConfigs.KafkaTopics;

import java.text.MessageFormat;
import java.util.UUID;

public final class ReqCar
        extends CassandraConverter
        implements ObjectFromRowConvertInterface< ReqCar >,
        KafkaEntitiesCommonMethods {
    public UUID getPatrulId() {
        return this.patrulId;
    }

    public void setPatrulId( final UUID uuid ) {
        this.patrulId = uuid;
    }

    public UUID getUuid() {
        return this.uuid;
    }

    public void setUuid ( final UUID uuid ) {
        this.uuid = uuid;
    }

    public UUID getLustraId() {
        return this.lustraId;
    }

    public void setLustraId ( final UUID lustraId ) {
        this.lustraId = lustraId;
    }

    public String getGosNumber() {
        return this.gosNumber;
    }

    public void setGosNumber ( final String gosNumber ) {
        this.gosNumber = gosNumber;
    }

    public String getTrackerId() {
        return this.trackerId;
    }

    public void setTrackerId ( final String trackerId ) {
        this.trackerId = trackerId;
    }

    public String getVehicleType() {
        return this.vehicleType;
    }

    public void setVehicleType ( final String vehicleType ) {
        this.vehicleType = vehicleType;
    }

    public String getCarImageLink() {
        return this.carImageLink;
    }

    public void setCarImageLink ( final String carImageLink ) {
        this.carImageLink = carImageLink;
    }

    public String getPatrulPassportSeries() {
        return this.patrulPassportSeries;
    }

    public void setPatrulPassportSeries ( final String patrulPassportSeries ) {
        this.patrulPassportSeries = patrulPassportSeries;
    }

    public int getSideNumber() {
        return this.sideNumber;
    }

    public void setSideNumber ( final int sideNumber ) {
        this.sideNumber = sideNumber;
    }

    public int getSimCardNumber() {
        return this.simCardNumber;
    }

    public void setSimCardNumber ( final int simCardNumber ) {
        this.simCardNumber = simCardNumber;
    }

    public double getLatitude() {
        return this.latitude;
    }

    public void setLatitude ( final double latitude ) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return this.longitude;
    }

    public void setLongitude ( final double longitude ) {
        this.longitude = longitude;
    }

    public double getAverageFuelSize() {
        return this.averageFuelSize;
    }

    public void setAverageFuelSize ( final double averageFuelSize ) {
        this.averageFuelSize = averageFuelSize;
    }

    public double getAverageFuelConsumption() {
        return this.averageFuelConsumption;
    }

    public void setAverageFuelConsumption ( final double averageFuelConsumption ) {
        this.averageFuelConsumption = averageFuelConsumption;
    }

    private UUID uuid;
    private UUID lustraId;
    private UUID patrulId;

    private String gosNumber;
    private String trackerId;
    private String vehicleType;
    private String carImageLink;
    private String patrulPassportSeries;

    private int sideNumber; // бортовой номер
    private int simCardNumber;

    private double latitude;
    private double longitude;
    private double averageFuelSize; // средний расход топлива по документам
    private double averageFuelConsumption = 0.0; // средний расход топлива исходя из стиля вождения водителя

    public ReqCar () {}

    @Override
    public CassandraTables getEntityTableName() {
        return CassandraTables.TABLETS;
    }

    @Override
    public CassandraTables getEntityKeyspaceName() {
        return CassandraTables.CARS;
    }

    @Override
    public ReqCar generate( final Row row ) {
        super.checkAndSetParams(
                row,
                row1 -> {
                    this.setUuid( row.getUUID( "uuid" ) );
                    this.setPatrulId( row.getUUID( "patrulId" ) );
                    this.setLustraId( row.getUUID( "lustraId" ) );

                    this.setGosNumber( row.getString( "gosNumber" ) );
                    this.setTrackerId( row.getString( "trackerId" ) );
                    this.setVehicleType( row.getString( "vehicleType" ) );
                    this.setCarImageLink( row.getString( "carImageLink" ) );
                    this.setPatrulPassportSeries( row.getString( "patrulPassportSeries" ) );

                    this.setSideNumber( row.getInt( "sideNumber" ) );
                    this.setSimCardNumber( row.getInt( "simCardNumber" ) );

                    this.setLatitude( row.getDouble( "latitude" ) );
                    this.setLongitude( row.getDouble( "longitude" ) );
                    this.setAverageFuelSize( row.getDouble( "averageFuelSize" ) );
                    this.setAverageFuelConsumption( row.getDouble( "averageFuelConsumption" ) );
                }
        );

        return this;
    }

    @Override
    public ReqCar generate() {
        return new ReqCar();
    }

    @Override
    public String getEntityInsertCommand() {
        return MessageFormat.format(
                """
                {0} {1} {2} {3}
                """,
                CassandraCommands.BEGIN_BATCH,

                /*
                обновляем данные патрульного чтобы связать его с машиной
                */
                MessageFormat.format(
                        """
                        {0} {1}.{2}
                        SET carNumber = {3}, carType = {4}, uuidForPatrulCar = {5}
                        WHERE uuid = {6};
                        """,
                        CassandraCommands.UPDATE,

                        this.getEntityKeyspaceName(),
                        CassandraTables.PATRULS,

                        super.joinWithAstrix( this.getGosNumber() ),
                        super.joinWithAstrix( this.getVehicleType() ),

                        this.getUuid(),
                        this.getPatrulId()
                ),

                /*
                сохраняем данные самой машины
                */
                MessageFormat.format(
                        """
                        {0} {1}.{2} {3}
                        VALUES ( {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16} {17} );
                        """,
                        CassandraCommands.INSERT_INTO,

                        this.getEntityKeyspaceName(),
                        this.getEntityTableName(),

                        super.getAllParamsNamesForClass.apply( ReqCar.class ),

                        CassandraFunctions.UUID,
                        this.getLustraId(),
                        this.getPatrulId(),

                        super.joinWithAstrix( this.getGosNumber() ),
                        super.joinWithAstrix( this.getTrackerId() ),
                        super.joinWithAstrix( this.getVehicleType() ),
                        super.joinWithAstrix( this.getCarImageLink() ),
                        super.joinWithAstrix( this.getPatrulPassportSeries() ),

                        this.getSideNumber(),
                        this.getSimCardNumber(),

                        this.getLatitude(),
                        this.getLongitude(),
                        this.getAverageFuelSize(),
                        this.getAverageFuelConsumption()
                ),
                CassandraCommands.APPLY_BATCH
        );
    }

    @Override
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

                        super.joinWithAstrix( this.getTrackerId() )
                ),

                CassandraCommands.APPLY_BATCH
        );
    }

    @Override
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
    public KafkaTopics getTopicName() {
        return KafkaTopics.NEW_CAR_TOPIC;
    }

    @Override
    public String getSuccessMessage() {
        return "Kafka got ReqCar: " + this.getTrackerId();
    }
}
