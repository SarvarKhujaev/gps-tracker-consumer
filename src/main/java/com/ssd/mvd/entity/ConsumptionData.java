package com.ssd.mvd.entity;

public final class ConsumptionData {
    public Double getFuelLevel() {
        return this.fuelLevel;
    }

    public void setFuelLevel( final Double fuelLevel ) {
        this.fuelLevel = fuelLevel;
    }

    public Double getDistance() {
        return this.distance;
    }

    public void setDistance( final Double distance ) {
        this.distance = distance;
    }

    private Double fuelLevel;
    private Double distance;

    public ConsumptionData () {}
}
