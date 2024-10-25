package com.ssd.mvd.entity.patrulDataSet.patrulSubClasses;

import java.util.UUID;
import com.datastax.driver.core.GettableData;

import com.ssd.mvd.inspectors.DataValidateInspector;
import com.ssd.mvd.interfaces.ObjectFromRowConvertInterface;

public final class PatrulUniqueValues implements ObjectFromRowConvertInterface< PatrulUniqueValues > {
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
    public PatrulUniqueValues generate() {
        return new PatrulUniqueValues();
    }

    @Override
    @lombok.NonNull
    public PatrulUniqueValues generate( @lombok.NonNull final GettableData udtValue ) {
        DataValidateInspector.checkAndSetParams(
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
