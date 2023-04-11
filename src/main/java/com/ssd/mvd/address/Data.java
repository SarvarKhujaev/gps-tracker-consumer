package com.ssd.mvd.address;

import lombok.extern.jackson.Jacksonized;

@lombok.Data
@Jacksonized
@lombok.NoArgsConstructor
@lombok.AllArgsConstructor
public class Data {
    private String road;
    private String city;
    private String county;
    private String country;
    private String postcode;
    private String residential;
    private String country_code;
}
