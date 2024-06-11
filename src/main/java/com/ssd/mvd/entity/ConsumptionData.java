package com.ssd.mvd.entity;

public final class ConsumptionData {
    public double getFuelLevel() {
        return this.fuelLevel;
    }

    public void setFuelLevel( final double fuelLevel ) {
        this.fuelLevel = fuelLevel;
    }

    public double getDistance() {
        return this.distance;
    }

    public void setDistance( final double distance ) {
        this.distance = distance;
    }

    private double fuelLevel;
    private double distance;

    public ConsumptionData () {}
}
