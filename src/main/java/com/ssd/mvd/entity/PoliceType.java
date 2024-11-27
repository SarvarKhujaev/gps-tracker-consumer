package com.ssd.mvd.entity;

import com.ssd.mvd.annotations.entity.object.EntityConstructorAnnotation;
import com.ssd.mvd.annotations.entity.object.EntityAnnotations;

import com.ssd.mvd.annotations.entity.method.MethodsAnnotations;
import com.ssd.mvd.annotations.entity.field.FieldAnnotation;

import com.ssd.mvd.interfaces.entity.ObjectFromRowConvertInterface;
import com.ssd.mvd.inspectors.AnnotationInspector;

import com.ssd.mvd.constants.cassandra.CassandraDataTypes;
import com.ssd.mvd.constants.cassandra.CassandraTables;

import java.util.UUID;

@EntityAnnotations(
        name = "PoliceType",
        comment = "тип патрульного, например YPS или PPX",
        tableName = CassandraTables.POLICE_TYPE
)
public final class PoliceType implements ObjectFromRowConvertInterface< PoliceType > {
    @MethodsAnnotations(
            name = "uuid",
            isPrimaryKey = true
    )
    @lombok.NonNull
    public UUID getUuid() {
        return this.uuid;
    }

    public String getIcon() {
        return this.icon;
    }

    public String getIcon2() {
        return this.icon2;
    }

    public String getPoliceType() {
        return this.policeType;
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
            name = "icon",
            withoutParams = false,
            isReturnEntity = false
    )
    public void setIcon ( final String icon ) {
        this.icon = icon;
    }

    @MethodsAnnotations(
            name = "icon2",
            withoutParams = false,
            isReturnEntity = false
    )
    public void setIcon2 ( final String icon2 ) {
        this.icon2 = icon2;
    }

    @MethodsAnnotations(
            name = "policeType",
            withoutParams = false,
            isReturnEntity = false
    )
    public void setPoliceType ( final String policeType ) {
        this.policeType = policeType;
    }

    @FieldAnnotation( name = "uuid", mightBeNull = false )
    private UUID uuid;
    @FieldAnnotation( name = "icon", hasToBeJoinedWithAstrix = true, mightBeNull = false )
    private String icon;
    @FieldAnnotation( name = "icon2", hasToBeJoinedWithAstrix = true, mightBeNull = false )
    private String icon2;
    @FieldAnnotation( name = "policeType", hasToBeJoinedWithAstrix = true, mightBeNull = false )
    private String policeType;

    private PoliceType () {}

    @EntityConstructorAnnotation
    public <T> PoliceType ( @lombok.NonNull final Class<T> instance ) {
        AnnotationInspector.checkCallerPermission( instance, PoliceType.class );
    }

    @Override
    @lombok.NonNull
    public PoliceType generate () {
        return new PoliceType();
    }

    @Override
    @lombok.NonNull
    @org.jetbrains.annotations.Contract( value = "_ -> fail" )
    public PoliceType generate( @lombok.NonNull final com.datastax.driver.core.GettableData gettableData ) {
        return AnnotationInspector.fillEntityParams( this, gettableData );
    }
}
