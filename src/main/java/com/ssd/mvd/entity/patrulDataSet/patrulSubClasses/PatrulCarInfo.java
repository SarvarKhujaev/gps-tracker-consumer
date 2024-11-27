package com.ssd.mvd.entity.patrulDataSet.patrulSubClasses;

import com.ssd.mvd.annotations.entity.object.EntityConstructorAnnotation;
import com.ssd.mvd.annotations.entity.object.EntityAnnotations;

import com.ssd.mvd.annotations.entity.method.MethodsAnnotations;
import com.ssd.mvd.annotations.entity.field.FieldAnnotation;

import com.ssd.mvd.interfaces.entity.ObjectFromRowConvertInterface;
import com.ssd.mvd.constants.cassandra.CassandraTables;
import com.ssd.mvd.inspectors.AnnotationInspector;

@EntityAnnotations( name = "PatrulCarInfo", isSubClass = true, tableName = CassandraTables.PATRUL_CAR_DATA )
public final class PatrulCarInfo implements ObjectFromRowConvertInterface< PatrulCarInfo > {
    public String getCarType() {
        return this.carType;
    }

    public String getCarNumber() {
        return this.carNumber;
    }

    @MethodsAnnotations(
            name = "carType",
            withoutParams = false,
            isReturnEntity = false
    )
    public void setCarType( final String carType ) {
        this.carType = carType;
    }

    @MethodsAnnotations(
            name = "carNumber",
            withoutParams = false,
            isReturnEntity = false
    )
    public void setCarNumber( final String carNumber ) {
        this.carNumber = carNumber;
    }

    @FieldAnnotation( name = "carType", hasToBeJoinedWithAstrix = true, comment = "модель машины" )
    private String carType;
    @FieldAnnotation( name = "carNumber", hasToBeJoinedWithAstrix = true )
    private String carNumber;

    private PatrulCarInfo () {}

    @EntityConstructorAnnotation
    public <T> PatrulCarInfo ( @lombok.NonNull final Class<T> instance ) {
        AnnotationInspector.checkCallerPermission( instance, PatrulCarInfo.class );
    }

    @Override
    @lombok.NonNull
    public PatrulCarInfo generate () {
        return new PatrulCarInfo();
    }

    @Override
    @lombok.NonNull
    @org.jetbrains.annotations.Contract( value = "_ -> fail" )
    public PatrulCarInfo generate( final com.datastax.driver.core.GettableData gettableData ) {
        return AnnotationInspector.fillEntityParams( this, gettableData );
    }
}
