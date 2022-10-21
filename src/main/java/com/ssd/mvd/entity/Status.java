package com.ssd.mvd.entity;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Status {
    private Integer code;
    private String message;
}
