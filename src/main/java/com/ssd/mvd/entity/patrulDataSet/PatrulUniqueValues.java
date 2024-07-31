package com.ssd.mvd.entity.patrulDataSet;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.UDTValue;
import com.ssd.mvd.inspectors.DataValidateInspector;
import com.ssd.mvd.interfaces.ObjectCommonMethods;

import java.util.UUID;

public final class PatrulUniqueValues extends DataValidateInspector implements ObjectCommonMethods< PatrulUniqueValues > {
    public void setOrgan( final UUID organ ) {
        this.organ = organ;
    }

    public void setSos_id( final UUID sos_id ) {
        this.sos_id = sos_id;
    }

    public void setUuidOfEscort( final UUID uuidOfEscort ) {
        this.uuidOfEscort = uuidOfEscort;
    }

    public UUID getUuidForPatrulCar() {
        return this.uuidForPatrulCar;
    }

    public void setUuidForPatrulCar( final UUID uuidForPatrulCar ) {
        this.uuidForPatrulCar = uuidForPatrulCar;
    }

    public UUID getUuidForEscortCar() {
        return this.uuidForEscortCar;
    }

    public void setUuidForEscortCar( final UUID uuidForEscortCar ) {
        this.uuidForEscortCar = uuidForEscortCar;
    }

    private UUID organ; // choosing from dictionary
    private UUID sos_id; // choosing from dictionary
    private UUID uuidOfEscort; // UUID of the Escort which this car is linked to
    private UUID uuidForPatrulCar; // choosing from dictionary
    private UUID uuidForEscortCar; // choosing from dictionary

    public PatrulUniqueValues () {}

    @Override
    public PatrulUniqueValues generate( final Row row ) {
        super.checkAndSetParams(
                row,
                row1 -> {
                    this.setUuidForEscortCar( row.getUUID( "uuidForEscortCar" ) );
                    this.setUuidForPatrulCar( row.getUUID( "uuidForPatrulCar" ) );
                    this.setUuidOfEscort( row.getUUID( "uuidOfEscort" ) );
                    this.setSos_id( row.getUUID( "sos_id" ) );
                    this.setOrgan( row.getUUID( "organ" ) );
                }
        );

        return this;
    }

    @Override
    public PatrulUniqueValues generate() {
        return new PatrulUniqueValues();
    }

    @Override
    public PatrulUniqueValues generate( final UDTValue udtValue ) {
        super.checkAndSetParams(
                udtValue,
                udtValue1 -> {
                    this.setOrgan( udtValue.getUUID( "organ" ) );
                    this.setSos_id( udtValue.getUUID( "sos_id" ) );
                    this.setUuidOfEscort( udtValue.getUUID( "uuidOfEscort" ) );
                    this.setUuidForPatrulCar( udtValue.getUUID( "uuidForPatrulCar" ) );
                    this.setUuidForEscortCar( udtValue.getUUID( "uuidForEscortCar" ) );
                }
        );

        return this;
    }
}
