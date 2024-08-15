package com.ssd.mvd.entity.patrulDataSet.patrulSubClasses;

import com.ssd.mvd.inspectors.CollectionsInspector;
import com.ssd.mvd.entity.ConsumptionData;

import java.util.SortedMap;
import java.util.Date;
import java.util.UUID;

public final class PatrulFuelStatistics extends CollectionsInspector {
    public UUID getUuid() {
        return this.uuid;
    }

    public void setUuid( final UUID uuid ) {
        this.uuid = uuid;
    }

    public double getAverageDistance() {
        return this.averageDistance;
    }

    public void setAverageDistance( final double averageDistance ) {
        this.averageDistance = averageDistance;
    }

    public double getAverageFuelConsumption() {
        return this.averageFuelConsumption;
    }

    public void setAverageFuelConsumption( final double averageFuelConsumption ) {
        this.averageFuelConsumption = averageFuelConsumption;
    }

    public SortedMap< Date, ConsumptionData > getMap() {
        return this.map;
    }

    public void setMap( final SortedMap< Date, ConsumptionData > map ) {
        this.map = map;
    }

    private UUID uuid;
    private double averageDistance = 0.0;
    private double averageFuelConsumption = 0.0;
    private SortedMap< Date, ConsumptionData > map = super.newTreeMap();

    public PatrulFuelStatistics () {}
}
