package com.ssd.mvd.entity;

import lombok.Data;
import java.util.*;

@Data
public class PatrulFuelStatistics {
    private UUID uuid; // patrul id
    private Double averageDistance = 0.0;
    private Double averageFuelConsumption = 0.0;
    private SortedMap< Date, ConsumptionData > map = new TreeMap<>();
}
