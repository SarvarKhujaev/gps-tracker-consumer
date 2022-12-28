package com.ssd.mvd.entity;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Data;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ConsumptionData {
    private Double fuelLevel;
    private Double distance;
}
