package com.ssd.mvd.entity.patrulDataSet.patrulSubClasses;

import com.datastax.driver.core.GettableData;
import com.ssd.mvd.inspectors.DataValidateInspector;
import com.ssd.mvd.interfaces.ObjectFromRowConvertInterface;

public final class PatrulLocationData implements ObjectFromRowConvertInterface< PatrulLocationData > {
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
    @lombok.NonNull
    public PatrulLocationData generate( @lombok.NonNull final GettableData row ) {
        DataValidateInspector.checkAndSetParams(
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
    @lombok.NonNull
    public PatrulLocationData generate() {
        return new PatrulLocationData();
    }
}
