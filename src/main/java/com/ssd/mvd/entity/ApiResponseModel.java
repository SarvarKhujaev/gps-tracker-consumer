package com.ssd.mvd.entity;

@lombok.Data
@lombok.Builder
public class ApiResponseModel {
    private Boolean success;
    private Status status;
}
