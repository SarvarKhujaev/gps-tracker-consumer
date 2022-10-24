package com.ssd.mvd.entity;

import java.util.*;

import lombok.Data;

@Data
public class PatrulFuelStatistics {
    private UUID uuid; // patrul id
    private SortedMap< Date, Double > map = new TreeMap<>();
}
