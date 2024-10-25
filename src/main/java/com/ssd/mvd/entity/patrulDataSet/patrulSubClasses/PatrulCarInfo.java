package com.ssd.mvd.entity.patrulDataSet.patrulSubClasses;

import com.ssd.mvd.interfaces.ObjectFromRowConvertInterface;
import com.ssd.mvd.inspectors.DataValidateInspector;
import com.ssd.mvd.entity.TupleOfCar;
import com.ssd.mvd.annotations.*;

import com.datastax.driver.core.GettableData;

@EntityAnnotations( name = "PatrulCarInfo", isSubClass = true )
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
    @lombok.NonNull
    public PatrulCarInfo generate( @lombok.NonNull final GettableData row ) {
        DataValidateInspector.checkAndSetParams(
                row,
                row1 -> {
                    this.setCarType( row.getString( "carType" ) );
                    this.setCarNumber( row.getString( "carNumber" ) );
                }
        );

        return this;
    }

    @Override
    @lombok.NonNull
    public PatrulCarInfo generate() {
        return new PatrulCarInfo();
    }
}
