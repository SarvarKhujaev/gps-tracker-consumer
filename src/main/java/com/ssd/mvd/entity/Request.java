package com.ssd.mvd.entity;

import java.util.Date;

@lombok.Data
@lombok.NoArgsConstructor
@lombok.AllArgsConstructor
public final class Request {
    private String trackerId;
    private Date startTime;
    private Date endTime;
}