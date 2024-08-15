package com.ssd.mvd.entity.patrulDataSet.patrulSubClasses;

import com.ssd.mvd.inspectors.DataValidateInspector;
import com.ssd.mvd.interfaces.ObjectCommonMethods;
import com.ssd.mvd.entity.TupleOfCar;
import com.ssd.mvd.annotations.*;

import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.Row;

@EntityAnnotations( name = "PatrulCarInfo", isSubClass = true )
public final class PatrulCarInfo extends DataValidateInspector implements ObjectCommonMethods< PatrulCarInfo > {
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

    public void setCarNumber( final TupleOfCar tupleOfCar ) {
        this.setCarNumber( tupleOfCar.getGosNumber() );
        this.setCarType( tupleOfCar.getCarModel() );
    }

    @FieldAnnotation( name = "carType", hasToBeJoinedWithAstrix = true )
    private String carType; // модель машины
    @FieldAnnotation( name = "carNumber", hasToBeJoinedWithAstrix = true )
    private String carNumber;

    public PatrulCarInfo () {}

    @Override
    public PatrulCarInfo generate( final UDTValue udtValue ) {
        super.checkAndSetParams(
                udtValue,
                udtValue1 -> {
                    this.setCarType( udtValue.getString( "carType" ) );
                    this.setCarNumber( udtValue.getString( "carNumber" ) );
                }
        );

        return this;
    }

    @Override
    public PatrulCarInfo generate( final Row row ) {
        super.checkAndSetParams(
                row,
                row1 -> {
                    this.setCarType( row.getString( "carType" ) );
                    this.setCarNumber( row.getString( "carNumber" ) );
                }
        );

        return this;
    }

    @Override
    public PatrulCarInfo generate() {
        return new PatrulCarInfo();
    }
}
