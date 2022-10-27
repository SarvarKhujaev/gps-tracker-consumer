package com.ssd.mvd.entity;

import lombok.Data;
import java.util.*;

@Data
public class PatrulFuelStatistics {
    private UUID uuid; // patrul id
    private Double averageFuelConsumption;
    private SortedMap< Date, ConsumptionData > map = new TreeMap<>();
}
