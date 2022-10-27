package com.ssd.mvd.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ConsumptionData {
    private Double fuelLevel;
    private Double distance;
}
