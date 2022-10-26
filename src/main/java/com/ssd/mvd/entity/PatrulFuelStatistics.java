package com.ssd.mvd.entity;

import lombok.Data;
import java.util.*;

@Data
public class PatrulFuelStatistics {
    private UUID uuid; // patrul id
    private SortedMap< Date, ConsumptionData > map = new TreeMap<>();
}
