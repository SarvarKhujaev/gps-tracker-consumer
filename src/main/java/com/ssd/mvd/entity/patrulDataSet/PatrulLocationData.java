package com.ssd.mvd.entity.patrulDataSet;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.UDTValue;

import com.ssd.mvd.interfaces.ObjectCommonMethods;
import com.ssd.mvd.inspectors.DataValidateInspector;

public final class PatrulLocationData extends DataValidateInspector implements ObjectCommonMethods< PatrulLocationData > {
    public double getDistance() {
        return this.distance;
    }

    public void setDistance( final double distance ) {
        this.distance = distance;
    }

    public double getLatitude() {
        return this.latitude;
    }

    public void setLatitude( final double latitude ) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return this.longitude;
    }

    public void setLongitude( final double longitude ) {
        this.longitude = longitude;
    }

    public double getLatitudeOfTask() {
        return this.latitudeOfTask;
    }

    public void setLatitudeOfTask( final double latitudeOfTask ) {
        this.latitudeOfTask = latitudeOfTask;
    }

    public double getLongitudeOfTask() {
        return this.longitudeOfTask;
    }

    public void setLongitudeOfTask( final double longitudeOfTask ) {
        this.longitudeOfTask = longitudeOfTask;
    }

    private double distance;
    // текущее местоположение патрульного по Х
    private double latitude;
    // текущее местоположение патрульного по Y
    private double longitude;
    // локация заданной задачи по Х
    private double latitudeOfTask;
    // локация заданной задачи по Y
    private double longitudeOfTask;

    public PatrulLocationData () {}

    @Override
    public PatrulLocationData generate( final Row row ) {
        super.checkAndSetParams(
                row,
                row1 -> {
                    this.setLongitudeOfTask( row.getDouble( "longitudeOfTask" ) );
                    this.setLatitudeOfTask( row.getDouble( "latitudeOfTask" ) );
                    this.setLongitude( row.getDouble( "longitude" ) );
                    this.setLatitude( row.getDouble( "latitude" ) );
                    this.setDistance( row.getDouble( "distance" ) );
                }
        );

        return this;
    }

    @Override
    public PatrulLocationData generate() {
        return new PatrulLocationData();
    }

    @Override
    public PatrulLocationData generate( final UDTValue udtValue ) {
        super.checkAndSetParams(
                udtValue,
                udtValue1 -> {
                    this.setDistance( udtValue.getDouble( "distance" ) );
                    this.setLatitude( udtValue.getDouble( "latitude" ) );
                    this.setLongitude( udtValue.getDouble( "longitude" ) );
                    this.setLatitudeOfTask( udtValue.getDouble( "latitudeOfTask" ) );
                    this.setLongitudeOfTask( udtValue.getDouble( "longitudeOfTask" ) );
                }
        );

        return this;
    }
}
