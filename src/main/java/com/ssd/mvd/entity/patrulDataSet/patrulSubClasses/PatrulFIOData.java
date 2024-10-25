package com.ssd.mvd.entity.patrulDataSet.patrulSubClasses;

import java.util.Optional;
import com.datastax.driver.core.GettableData;

import com.ssd.mvd.annotations.FieldAnnotation;
import com.ssd.mvd.annotations.EntityAnnotations;
import com.ssd.mvd.annotations.MethodsAnnotations;

import com.ssd.mvd.inspectors.DataValidateInspector;
import com.ssd.mvd.interfaces.ObjectFromRowConvertInterface;

@EntityAnnotations( name = "PatrulFIOData", isSubClass = true )
public final class PatrulFIOData extends DataValidateInspector implements ObjectFromRowConvertInterface< PatrulFIOData > {
    public String getName() {
        return this.name;
    }

    public String getSurname() {
        return this.surname;
    }

    public String getFatherName() {
        return this.fatherName;
    }

    @MethodsAnnotations(
            name = "name",
            withoutParams = false,
            isReturnEntity = false
    )
    public void setName( final String name ) {
        this.name = name;
    }

    @MethodsAnnotations(
            name = "surname",
            withoutParams = false,
            isReturnEntity = false
    )
    public void setSurname( final String surname ) {
        this.surname = surname;
    }

    @MethodsAnnotations(
            name = "fatherName",
            withoutParams = false,
            isReturnEntity = false
    )
    public void setFatherName( final String fatherName ) {
        this.fatherName = fatherName;
    }

    @MethodsAnnotations(
            name = "surnameNameFatherName",
            withoutParams = false,
            isReturnEntity = false
    )
    public void setSurnameNameFatherName (
            final String surnameNameFatherName
    ) {
        this.surnameNameFatherName = surnameNameFatherName;
    }

    public String getSurnameNameFatherName () {
        return Optional.ofNullable( this.surnameNameFatherName )
                .filter( s -> this.surnameNameFatherName.contains("NULL") )
                .orElse( ( this.surnameNameFatherName = super.concatNames( this ) ) );
    }

    @FieldAnnotation( name = "name", hasToBeJoinedWithAstrix = true, mightBeNull = false )
    private String name;
    @FieldAnnotation( name = "surname", hasToBeJoinedWithAstrix = true, mightBeNull = false )
    private String surname;
    @FieldAnnotation( name = "fatherName", hasToBeJoinedWithAstrix = true, mightBeNull = false )
    private String fatherName;
    @FieldAnnotation( name = "surnameNameFatherName", hasToBeJoinedWithAstrix = true, mightBeNull = false )
    private String surnameNameFatherName; // Ф.И.О

    public PatrulFIOData () {}

    @Override
    @lombok.NonNull
    public PatrulFIOData generate( @lombok.NonNull final GettableData gettableData ) {
        checkAndSetParams(
                gettableData,
                row1 -> {
                    this.setSurnameNameFatherName( gettableData.getString( "surnameNameFatherName" ) );
                    this.setFatherName( gettableData.getString( "fatherName" ) );
                    this.setSurname( gettableData.getString( "surname" ) );
                    this.setName( gettableData.getString( "name" ) );
                }
        );

        return this;
    }

    @Override
    @lombok.NonNull
    public PatrulFIOData generate() {
        return new PatrulFIOData();
    }
}
