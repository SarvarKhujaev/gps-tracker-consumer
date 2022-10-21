package com.ssd.mvd.entity;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import java.util.Date;
import lombok.Data;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Request {
    private String trackerId;
    private Date startTime;
    private Date endTime;
}