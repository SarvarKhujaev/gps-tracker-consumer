package com.ssd.mvd.entity;

import java.util.Date;
import java.util.HashMap;
import java.util.UUID;
import java.util.Map;
import lombok.Data;

@Data
public class PatrulFuelStatistics {
    private UUID uuid; // patrul id
    private Map< Date, Double > map = new HashMap<>();
}
