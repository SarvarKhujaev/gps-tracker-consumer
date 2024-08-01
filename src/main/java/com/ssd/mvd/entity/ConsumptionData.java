package com.ssd.mvd.entity;

public final class ConsumptionData {
    public double getDistance() {
        return this.distance;
    }

    public double getFuelLevel() {
        return this.fuelLevel;
    }

    public void setDistance( final double distance ) {
        this.distance = distance;
    }

    public void setFuelLevel( final double fuelLevel ) {
        this.fuelLevel = fuelLevel;
    }

    private double fuelLevel;
    private double distance;

    public ConsumptionData () {}
}
