package com.ssd.mvd.entity;

@lombok.Data
@lombok.Builder
public final class ApiResponseModel {
    private boolean success;
    private Status status;
}
