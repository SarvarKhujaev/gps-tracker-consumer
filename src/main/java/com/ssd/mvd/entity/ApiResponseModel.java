package com.ssd.mvd.entity;

@lombok.Data
@lombok.Builder
public final class ApiResponseModel {
    private Boolean success;
    private Status status;
}
