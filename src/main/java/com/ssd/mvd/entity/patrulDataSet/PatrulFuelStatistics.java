package com.ssd.mvd.entity.patrulDataSet;

import com.ssd.mvd.entity.ConsumptionData;
import java.util.*;

public final class PatrulFuelStatistics {
    public UUID getUuid() {
        return this.uuid;
    }

    public void setUuid( final UUID uuid ) {
        this.uuid = uuid;
    }

    public Double getAverageDistance() {
        return this.averageDistance;
    }

    public void setAverageDistance( final Double averageDistance ) {
        this.averageDistance = averageDistance;
    }

    public Double getAverageFuelConsumption() {
        return this.averageFuelConsumption;
    }

    public void setAverageFuelConsumption( final Double averageFuelConsumption ) {
        this.averageFuelConsumption = averageFuelConsumption;
    }

    public SortedMap< Date, ConsumptionData > getMap() {
        return this.map;
    }

    public void setMap( final SortedMap< Date, ConsumptionData > map ) {
        this.map = map;
    }

    private UUID uuid; // patrul id
    private Double averageDistance = 0.0;
    private Double averageFuelConsumption = 0.0;
    private SortedMap< Date, ConsumptionData > map = new TreeMap<>();
}
