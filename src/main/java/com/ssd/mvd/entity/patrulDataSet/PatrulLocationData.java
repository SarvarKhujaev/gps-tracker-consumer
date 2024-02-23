package com.ssd.mvd.entity.patrulDataSet;

import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.Row;

public final class PatrulLocationData {
    public Double getDistance() {
        return this.distance;
    }

    public void setDistance( final Double distance ) {
        this.distance = distance;
    }

    public Double getLatitude() {
        return this.latitude;
    }

    public void setLatitude( final Double latitude ) {
        this.latitude = latitude;
    }

    public Double getLongitude() {
        return this.longitude;
    }

    public void setLongitude( final Double longitude ) {
        this.longitude = longitude;
    }

    public Double getLatitudeOfTask() {
        return this.latitudeOfTask;
    }

    public void setLatitudeOfTask( final Double latitudeOfTask ) {
        this.latitudeOfTask = latitudeOfTask;
    }

    public Double getLongitudeOfTask() {
        return this.longitudeOfTask;
    }

    public void setLongitudeOfTask( final Double longitudeOfTask ) {
        this.longitudeOfTask = longitudeOfTask;
    }

    private Double distance;
    // текущее местоположение патрульного по Х
    private Double latitude;
    // текущее местоположение патрульного по Y
    private Double longitude;
    // локация заданной задачи по Х
    private Double latitudeOfTask;
    // локация заданной задачи по Y
    private Double longitudeOfTask;

    public static <T> PatrulLocationData generate ( final T object ) {
        return object instanceof Row
                ? new PatrulLocationData( (Row) object )
                : new PatrulLocationData( (UDTValue) object );
    }

    private PatrulLocationData ( final Row row ) {
        this.setDistance( row.getDouble( "distance" ) );
        this.setLatitude( row.getDouble( "latitude" ) );
        this.setLongitude( row.getDouble( "longitude" ) );
        this.setLatitudeOfTask( row.getDouble( "latitudeOfTask" ) );
        this.setLongitudeOfTask( row.getDouble( "longitudeOfTask" ) );
    }

    private PatrulLocationData( final UDTValue udtValue ) {
        this.setDistance( udtValue.getDouble( "distance" ) );
        this.setLatitude( udtValue.getDouble( "latitude" ) );
        this.setLongitude( udtValue.getDouble( "longitude" ) );
        this.setLatitudeOfTask( udtValue.getDouble( "latitudeOfTask" ) );
        this.setLongitudeOfTask( udtValue.getDouble( "longitudeOfTask" ) );
    }
}
