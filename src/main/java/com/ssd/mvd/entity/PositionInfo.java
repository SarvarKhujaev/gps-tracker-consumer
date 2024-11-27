package com.ssd.mvd.entity;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.ssd.mvd.annotations.entity.object.EntityConstructorAnnotation;
import com.ssd.mvd.annotations.entity.object.EntityAnnotations;

import com.ssd.mvd.annotations.entity.method.MethodsAnnotations;
import com.ssd.mvd.annotations.entity.field.FieldAnnotation;

import com.ssd.mvd.interfaces.entity.ObjectFromRowConvertInterface;
import com.ssd.mvd.inspectors.AnnotationInspector;
import com.ssd.mvd.controller.UnirestController;

import com.ssd.mvd.constants.cassandra.CassandraDataTypes;
import com.ssd.mvd.constants.cassandra.CassandraTables;

import java.util.Date;

@EntityAnnotations(
        name = "PositionInfo",
        comment = "хранит исторические данные о передвижениях машины",
        tableName = CassandraTables.POSITION_INFO
)
public final class PositionInfo implements ObjectFromRowConvertInterface< PositionInfo > {
    public double getLat() {
        return this.lat;
    }

    @MethodsAnnotations(
            name = "latitude",
            withoutParams = false,
            isReturnEntity = false,
            acceptEntityType = CassandraDataTypes.DOUBLE
    )
    public void setLat( final double lat ) {
        this.lat = lat;
    }

    public double getLng() {
        return this.lng;
    }

    @MethodsAnnotations(
            name = "longitude",
            withoutParams = false,
            isReturnEntity = false,
            acceptEntityType = CassandraDataTypes.DOUBLE
    )
    public void setLng( final double lng ) {
        this.lng = lng;
    }

    public double getSpeed() {
        return this.speed;
    }

    @MethodsAnnotations(
            name = "speed",
            withoutParams = false,
            isReturnEntity = false,
            acceptEntityType = CassandraDataTypes.DOUBLE
    )
    public void setSpeed( final double speed ) {
        this.speed = speed;
    }

    public void setAddress( final String address ) {
        this.address = address;
    }

    public Date getPositionWasSavedDate() {
        return this.positionWasSavedDate;
    }

    @MethodsAnnotations(
            name = "date",
            withoutParams = false,
            isReturnEntity = false,
            acceptEntityType = CassandraDataTypes.TIMESTAMP
    )
    public void setPositionWasSavedDate( final Date positionWasSavedDate ) {
        this.positionWasSavedDate = positionWasSavedDate;
    }

    @FieldAnnotation(
            name = "latitude",
            cassandraType = CassandraDataTypes.DOUBLE
    )
    private double lat;

    @FieldAnnotation(
            name = "longitude",
            cassandraType = CassandraDataTypes.DOUBLE
    )
    private double lng;

    @FieldAnnotation(
            name = "speed",
            cassandraType = CassandraDataTypes.DOUBLE
    )
    private double speed;

    private String address;

    @FieldAnnotation(
            name = "date",
            cassandraType = CassandraDataTypes.TIMESTAMP,
            hasToBeJoinedWithAstrix = true
    )
    private Date positionWasSavedDate;

    private PositionInfo () {}

    @EntityConstructorAnnotation
    public <T> PositionInfo ( final Class<T> instance ) {
        AnnotationInspector.checkCallerPermission( instance, PositionInfo.class );
    }

    public PositionInfo (
            @lombok.NonNull final com.datastax.driver.core.GettableData row,
            final boolean flag
    ) {
        this.generate( row );

        if ( flag ) {
            this.setAddress(
                    UnirestController
                            .getInstance()
                            .getAddressByLocation
                            .apply( this.getLat(), this.getLng() )
            );
        }
    }

    @Override
    @lombok.NonNull
    public PositionInfo generate() {
        return new PositionInfo();
    }

    @Override
    @lombok.NonNull
    public PositionInfo generate( @lombok.NonNull final com.datastax.driver.core.GettableData gettableData ) {
        return AnnotationInspector.fillEntityParams( this, gettableData );
    }

    @Override
    @lombok.NonNull
    @org.jetbrains.annotations.Contract( value = "_ -> !null" )
    public Select getEntitySelect (
            final Object ... params
    ) {
        return this.startSelect()
                .all()
                .where(
                        Relation.column(
                                CqlIdentifier.fromCql( "imei" )
                        ).isEqualTo( QueryBuilder.literal( params[0] ) ),
                        Relation.column(
                                CqlIdentifier.fromCql( "date" )
                        ).isGreaterThanOrEqualTo( QueryBuilder.literal( params[1] ) ),
                        Relation.column(
                                CqlIdentifier.fromCql( "date" )
                        ).isLessThanOrEqualTo( QueryBuilder.literal( params[2] ) )
                );
    }
}
