package com.ssd.mvd.entity.patrulDataSet;

import com.datastax.driver.core.UDTValue;
import com.ssd.mvd.entity.TupleOfCar;
import com.datastax.driver.core.Row;

public final class PatrulCarInfo {
    public String getCarType() {
        return this.carType;
    }

    public void setCarType( final String carType ) {
        this.carType = carType;
    }

    public String getCarNumber() {
        return this.carNumber;
    }

    public void setCarNumber( final String carNumber ) {
        this.carNumber = carNumber;
    }

    public void setCarNumber( final TupleOfCar tupleOfCar ) {
        this.setCarNumber( tupleOfCar.getGosNumber() );
        this.setCarType( tupleOfCar.getCarModel() );
    }

    private String carType; // модель машины
    private String carNumber;

    public static <T> PatrulCarInfo generate ( final T object ) {
        return object instanceof Row
                ? new PatrulCarInfo( (Row) object )
                : new PatrulCarInfo( (UDTValue) object );
    }

    private PatrulCarInfo ( final Row row ) {
        this.setCarType( row.getString( "carType" ) );
        this.setCarNumber( row.getString( "carNumber" ) );
    }

    private PatrulCarInfo( final UDTValue udtValue ) {
        this.setCarType( udtValue.getString( "carType" ) );
        this.setCarNumber( udtValue.getString( "carNumber" ) );
    }
}
