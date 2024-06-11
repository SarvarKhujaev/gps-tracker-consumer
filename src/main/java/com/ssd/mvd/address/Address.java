package com.ssd.mvd.address;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.List;

@lombok.Data
@lombok.NoArgsConstructor
@lombok.AllArgsConstructor
public final class Address {
    private long osm_id;
    private long place_id;

    private double lat;
    private double lon;
    private double importance;

    private String type;
    private String licence;
    private String osm_type;
    private String display_name;

    @JsonDeserialize
    private List< String > boundingbox;
    @JsonDeserialize
    private com.ssd.mvd.address.Data address;
}
