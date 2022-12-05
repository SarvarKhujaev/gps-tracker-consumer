package com.ssd.mvd.address;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.jackson.Jacksonized;

@lombok.Data
@Jacksonized
@NoArgsConstructor
@AllArgsConstructor
public class Data {
    private String road;
    private String city;
    private String county;
    private String country;
    private String postcode;
    private String residential;
    private String country_code;
}
