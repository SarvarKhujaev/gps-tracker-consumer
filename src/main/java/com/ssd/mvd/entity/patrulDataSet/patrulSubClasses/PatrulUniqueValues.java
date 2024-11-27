package com.ssd.mvd.entity.patrulDataSet.patrulSubClasses;

import java.util.UUID;

import com.ssd.mvd.annotations.entity.field.FieldAnnotation;
import com.ssd.mvd.annotations.entity.method.MethodsAnnotations;

import com.ssd.mvd.annotations.entity.object.EntityAnnotations;
import com.ssd.mvd.annotations.entity.object.EntityConstructorAnnotation;

import com.ssd.mvd.constants.cassandra.CassandraTables;
import com.ssd.mvd.constants.cassandra.CassandraDataTypes;

import com.ssd.mvd.inspectors.AnnotationInspector;
import com.ssd.mvd.interfaces.entity.ObjectFromRowConvertInterface;

@EntityAnnotations( name = "patrulUniqueValues", isSubClass = true, tableName = CassandraTables.PATRUL_UNIQUE_DATA )
public final class PatrulUniqueValues implements ObjectFromRowConvertInterface< PatrulUniqueValues > {
    @MethodsAnnotations(
            name = "organ",
            withoutParams = false,
            isReturnEntity = false,
            acceptEntityType = CassandraDataTypes.UUID
    )
    public void setOrgan( final UUID organ ) {
        this.organ = organ;
    }

    @MethodsAnnotations(
            name = "sos_id",
            withoutParams = false,
            isReturnEntity = false,
            acceptEntityType = CassandraDataTypes.UUID
    )
    public void setSos_id( final UUID sos_id ) {
        this.sos_id = sos_id;
    }

    @MethodsAnnotations(
            name = "uuidOfEscort",
            withoutParams = false,
            isReturnEntity = false,
            acceptEntityType = CassandraDataTypes.UUID
    )
    public void setUuidOfEscort( final UUID uuidOfEscort ) {
        this.uuidOfEscort = uuidOfEscort;
    }

    @MethodsAnnotations(
            name = "uuidForPatrulCar",
            withoutParams = false,
            isReturnEntity = false,
            acceptEntityType = CassandraDataTypes.UUID
    )
    public void setUuidForPatrulCar( final UUID uuidForPatrulCar ) {
        this.uuidForPatrulCar = uuidForPatrulCar;
    }

    @MethodsAnnotations(
            name = "uuidForEscortCar",
            withoutParams = false,
            isReturnEntity = false,
            acceptEntityType = CassandraDataTypes.UUID
    )
    public void setUuidForEscortCar( final UUID uuidForEscortCar ) {
        this.uuidForEscortCar = uuidForEscortCar;
    }

    public UUID getOrgan() {
        return this.organ;
    }

    public UUID getSos_id() {
        return this.sos_id;
    }

    public UUID getUuidOfEscort() {
        return this.uuidOfEscort;
    }

    public UUID getUuidForPatrulCar() {
        return this.uuidForPatrulCar;
    }

    public UUID getUuidForEscortCar() {
        return this.uuidForEscortCar;
    }

    @FieldAnnotation( name = "organ", comment = "choosing from dictionary" )
    private UUID organ;
    @FieldAnnotation( name = "sos_id", comment = "choosing from dictionary" )
    private UUID sos_id;
    @FieldAnnotation( name = "uuidOfEscort", comment = "UUID of the Escort which this car is linked to" )
    private UUID uuidOfEscort;
    @FieldAnnotation( name = "uuidForPatrulCar", comment = "choosing from dictionary" )
    private UUID uuidForPatrulCar;
    @FieldAnnotation( name = "uuidForEscortCar", comment = "choosing from dictionary" )
    private UUID uuidForEscortCar;

    private PatrulUniqueValues () {}

    @EntityConstructorAnnotation
    public <T> PatrulUniqueValues ( @lombok.NonNull final Class<T> instance ) {
        AnnotationInspector.checkCallerPermission( instance, PatrulUniqueValues.class );
    }

    @Override
    @lombok.NonNull
    public PatrulUniqueValues generate () {
        return new PatrulUniqueValues();
    }

    @Override
    @lombok.NonNull
    @org.jetbrains.annotations.Contract( value = "_ -> fail" )
    public PatrulUniqueValues generate( final com.datastax.driver.core.GettableData gettableData ) {
        return AnnotationInspector.fillEntityParams( this, gettableData );
    }
}
