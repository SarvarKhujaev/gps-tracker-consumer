package com.ssd.mvd.entity.patrulDataSet.patrulSubClasses;

import com.ssd.mvd.annotations.entity.field.FieldAnnotation;
import com.ssd.mvd.annotations.entity.method.MethodsAnnotations;

import com.ssd.mvd.annotations.entity.object.EntityAnnotations;
import com.ssd.mvd.annotations.entity.object.EntityConstructorAnnotation;

import com.ssd.mvd.inspectors.AnnotationInspector;
import com.ssd.mvd.inspectors.DataValidateInspector;
import com.ssd.mvd.inspectors.dataTypesInpectors.StringOperations;

import com.ssd.mvd.constants.cassandra.CassandraTables;
import com.ssd.mvd.interfaces.entity.ObjectFromRowConvertInterface;

@EntityAnnotations( name = "PatrulFIOData", isSubClass = true, tableName = CassandraTables.PATRUL_FIO_DATA )
public final class PatrulFIOData extends StringOperations implements ObjectFromRowConvertInterface< PatrulFIOData > {
    @lombok.NonNull
    public String getName() {
        return this.name;
    }

    @lombok.NonNull
    public String getSurname() {
        return this.surname;
    }

    @lombok.NonNull
    public String getFatherName() {
        return this.fatherName;
    }

    @lombok.NonNull
    public String getSurnameNameFatherName () {
        return DataValidateInspector.getOptional( this.surnameNameFatherName )
                .filter( s -> this.surnameNameFatherName.contains("NULL") )
                .orElse( ( this.surnameNameFatherName = super.concatNames( this ) ) );
    }

    @MethodsAnnotations(
            name = "name",
            withoutParams = false,
            isReturnEntity = false
    )
    public void setName( @lombok.NonNull final String name ) {
        this.name = name;
    }

    @MethodsAnnotations(
            name = "surname",
            withoutParams = false,
            isReturnEntity = false
    )
    public void setSurname( @lombok.NonNull final String surname ) {
        this.surname = surname;
    }

    @MethodsAnnotations(
            name = "fatherName",
            withoutParams = false,
            isReturnEntity = false
    )
    public void setFatherName( @lombok.NonNull final String fatherName ) {
        this.fatherName = fatherName;
    }

    @MethodsAnnotations(
            name = "surnameNameFatherName",
            withoutParams = false,
            isReturnEntity = false
    )
    public void setSurnameNameFatherName (
            @lombok.NonNull final String surnameNameFatherName
    ) {
        this.surnameNameFatherName = surnameNameFatherName;
    }

    @FieldAnnotation( name = "name", hasToBeJoinedWithAstrix = true, mightBeNull = false )
    private String name;
    @FieldAnnotation( name = "surname", hasToBeJoinedWithAstrix = true, mightBeNull = false )
    private String surname;
    @FieldAnnotation( name = "fatherName", hasToBeJoinedWithAstrix = true, mightBeNull = false )
    private String fatherName;
    @FieldAnnotation(
            name = "surnameNameFatherName",
            comment = "Ф.И.О патрульного",
            mightBeNull = false,
            hasToBeJoinedWithAstrix = true
    )
    private String surnameNameFatherName;

    private PatrulFIOData () {}

    @EntityConstructorAnnotation
    public <T> PatrulFIOData ( @lombok.NonNull final Class<T> instance ) {
        AnnotationInspector.checkCallerPermission( instance, PatrulFIOData.class );
    }

    @Override
    @lombok.NonNull
    public PatrulFIOData generate () {
        return new PatrulFIOData();
    }

    @Override
    @lombok.NonNull
    @org.jetbrains.annotations.Contract( value = "_ -> fail" )
    public PatrulFIOData generate( @lombok.NonNull final com.datastax.driver.core.GettableData gettableData ) {
        return AnnotationInspector.fillEntityParams( this, gettableData );
    }
}
