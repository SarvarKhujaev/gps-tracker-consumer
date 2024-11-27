package com.ssd.mvd.entity.patrulDataSet.patrulSubClasses;

import com.ssd.mvd.annotations.entity.object.EntityConstructorAnnotation;
import com.ssd.mvd.annotations.entity.object.EntityAnnotations;

import com.ssd.mvd.annotations.entity.method.MethodsAnnotations;
import com.ssd.mvd.annotations.entity.field.FieldAnnotation;

import com.ssd.mvd.interfaces.entity.ObjectFromRowConvertInterface;
import com.ssd.mvd.inspectors.AnnotationInspector;

import com.ssd.mvd.constants.cassandra.CassandraDataTypes;
import com.ssd.mvd.constants.cassandra.CassandraTables;

@EntityAnnotations(
        name = "patrulLocationData",
        isSubClass = true,
        tableName = CassandraTables.PATRUL_LOCATION_DATA
)
public final class PatrulLocationData implements ObjectFromRowConvertInterface< PatrulLocationData > {
    public double getDistance() {
        return this.distance;
    }

    public double getLatitude() {
        return this.latitude;
    }

    public double getLongitude() {
        return this.longitude;
    }

    public double getLatitudeOfTask() {
        return this.latitudeOfTask;
    }

    public double getLongitudeOfTask() {
        return this.longitudeOfTask;
    }

    @MethodsAnnotations(
            name = "distance",
            withoutParams = false,
            isReturnEntity = false,
            acceptEntityType = CassandraDataTypes.DOUBLE
    )
    public void setDistance( final double distance ) {
        this.distance = distance;
    }

    @MethodsAnnotations(
            name = "latitude",
            withoutParams = false,
            isReturnEntity = false,
            acceptEntityType = CassandraDataTypes.DOUBLE
    )
    public void setLatitude( final double latitude ) {
        this.latitude = latitude;
    }

    @MethodsAnnotations(
            name = "longitude",
            withoutParams = false,
            isReturnEntity = false,
            acceptEntityType = CassandraDataTypes.DOUBLE
    )
    public void setLongitude( final double longitude ) {
        this.longitude = longitude;
    }

    @MethodsAnnotations(
            name = "latitudeOfTask",
            withoutParams = false,
            isReturnEntity = false,
            acceptEntityType = CassandraDataTypes.DOUBLE
    )
    public void setLatitudeOfTask( final double latitudeOfTask ) {
        this.latitudeOfTask = latitudeOfTask;
    }

    @MethodsAnnotations(
            name = "longitudeOfTask",
            withoutParams = false,
            isReturnEntity = false,
            acceptEntityType = CassandraDataTypes.DOUBLE
    )
    public void setLongitudeOfTask( final double longitudeOfTask ) {
        this.longitudeOfTask = longitudeOfTask;
    }

    @FieldAnnotation( name = "distance" )
    private double distance;
    @FieldAnnotation( name = "latitude", comment = "текущее местоположение патрульного по Х" )
    private double latitude;
    @FieldAnnotation( name = "longitude", comment = "текущее местоположение патрульного по Y" )
    private double longitude;
    @FieldAnnotation( name = "latitudeOfTask", comment = "локация заданной задачи по Х" )
    private double latitudeOfTask;
    @FieldAnnotation( name = "longitudeOfTask", comment = "локация заданной задачи по Y" )
    private double longitudeOfTask;

    private PatrulLocationData () {}

    @EntityConstructorAnnotation
    public <T> PatrulLocationData ( @lombok.NonNull final Class<T> instance ) {
        AnnotationInspector.checkCallerPermission( instance, PatrulLocationData.class );
    }

    @Override
    @lombok.NonNull
    public PatrulLocationData generate () {
        return new PatrulLocationData();
    }

    @Override
    @lombok.NonNull
    @org.jetbrains.annotations.Contract( value = "_ -> fail" )
    public PatrulLocationData generate( @lombok.NonNull final com.datastax.driver.core.GettableData gettableData ) {
        return AnnotationInspector.fillEntityParams( this, gettableData );
    }
}
