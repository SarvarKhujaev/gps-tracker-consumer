package com.ssd.mvd.entity.patrulDataSet.patrulSubClasses;

import com.ssd.mvd.annotations.EntityAnnotations;
import com.ssd.mvd.annotations.FieldAnnotation;
import com.ssd.mvd.annotations.MethodsAnnotations;
import com.ssd.mvd.inspectors.DataValidateInspector;
import com.ssd.mvd.interfaces.ObjectCommonMethods;

import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.Row;

import java.util.Optional;

@EntityAnnotations( name = "PatrulFIOData", isSubClass = true )
public final class PatrulFIOData extends DataValidateInspector implements ObjectCommonMethods< PatrulFIOData > {
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
    public PatrulFIOData generate( final Row row ) {
        super.checkAndSetParams(
                row,
                row1 -> {
                    this.setSurnameNameFatherName( row.getString( "surnameNameFatherName" ) );
                    this.setFatherName( row.getString( "fatherName" ) );
                    this.setSurname( row.getString( "surname" ) );
                    this.setName( row.getString( "name" ) );
                }
        );

        return this;
    }

    @Override
    public PatrulFIOData generate() {
        return new PatrulFIOData();
    }

    @Override
    public PatrulFIOData generate( final UDTValue udtValue ) {
        super.checkAndSetParams(
                udtValue,
                udtValue1 -> {
                    this.setSurnameNameFatherName( udtValue.getString( "surnameNameFatherName" ) );
                    this.setFatherName( udtValue.getString( "fatherName" ) );
                    this.setSurname( udtValue.getString( "surname" ) );
                    this.setName( udtValue.getString( "name" ) );
                }
        );

        return this;
    }
}
