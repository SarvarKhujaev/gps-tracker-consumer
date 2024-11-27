package com.ssd.mvd.entity.patrulDataSet.patrulSubClasses;

import com.ssd.mvd.annotations.entity.object.EntityConstructorAnnotation;
import com.ssd.mvd.annotations.entity.object.EntityAnnotations;

import com.ssd.mvd.annotations.entity.method.MethodsAnnotations;
import com.ssd.mvd.annotations.entity.field.FieldAnnotation;

import com.ssd.mvd.interfaces.entity.ObjectFromRowConvertInterface;
import com.ssd.mvd.inspectors.AnnotationInspector;

import com.ssd.mvd.constants.cassandra.CassandraDataTypes;
import com.ssd.mvd.constants.cassandra.CassandraTables;

@EntityAnnotations( name = "patrulRegionData", isSubClass = true, tableName = CassandraTables.PATRUL_REGION_DATA )
public final class PatrulRegionData implements ObjectFromRowConvertInterface< PatrulRegionData > {
    public long getRegionId() {
        return this.regionId;
    }

    public long getMahallaId() {
        return this.mahallaId;
    }

    public long getDistrictId() {
        return this.districtId;
    }

    public String getRegionName() {
        return this.regionName;
    }

    public String getDistrictName() {
        return this.districtName;
    }

    @MethodsAnnotations(
            name = "regionId",
            withoutParams = false,
            isReturnEntity = false,
            acceptEntityType = CassandraDataTypes.BIGINT
    )
    public void setRegionId( final long regionId ) {
        this.regionId = regionId;
    }

    @MethodsAnnotations(
            name = "mahallaId",
            withoutParams = false,
            isReturnEntity = false,
            acceptEntityType = CassandraDataTypes.BIGINT
    )
    public void setMahallaId( final long mahallaId ) {
        this.mahallaId = mahallaId;
    }

    @MethodsAnnotations(
            name = "districtId",
            withoutParams = false,
            isReturnEntity = false,
            acceptEntityType = CassandraDataTypes.BIGINT
    )
    public void setDistrictId( final long districtId ) {
        this.districtId = districtId;
    }

    @MethodsAnnotations(
            name = "regionName",
            withoutParams = false,
            isReturnEntity = false
    )
    public void setRegionName( final String regionName ) {
        this.regionName = regionName;
    }

    @MethodsAnnotations(
            name = "districtName",
            withoutParams = false,
            isReturnEntity = false
    )
    public void setDistrictName( final String districtName ) {
        this.districtName = districtName;
    }

    @FieldAnnotation( name = "regionId" )
    private long regionId;
    @FieldAnnotation( name = "mahallaId" )
    private long mahallaId;
    @FieldAnnotation( name = "districtId" )
    private long districtId;

    @FieldAnnotation( name = "regionName", hasToBeJoinedWithAstrix = true )
    private String regionName;
    @FieldAnnotation( name = "districtName", hasToBeJoinedWithAstrix = true )
    private String districtName;

    private PatrulRegionData () {}

    @EntityConstructorAnnotation
    public <T> PatrulRegionData ( @lombok.NonNull final Class<T> instance ) {
        AnnotationInspector.checkCallerPermission( instance, PatrulRegionData.class );
    }

    @Override
    @lombok.NonNull
    public PatrulRegionData generate () {
        return new PatrulRegionData();
    }

    @Override
    @lombok.NonNull
    @org.jetbrains.annotations.Contract( value = "_ -> fail" )
    public PatrulRegionData generate( @lombok.NonNull final com.datastax.driver.core.GettableData gettableData ) {
        return AnnotationInspector.fillEntityParams( this, gettableData );
    }
}
