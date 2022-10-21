package com.ssd.mvd.entity;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ApiResponseModel {
    private com.ssd.mvd.entity.Data data;
    private Boolean success;
    private Status status;
}
