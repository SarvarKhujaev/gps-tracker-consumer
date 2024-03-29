package com.ssd.mvd.address;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.List;

@lombok.Data
@lombok.NoArgsConstructor
@lombok.AllArgsConstructor
public final class Address {
    private Long osm_id;
    private Long place_id;

    private Double lat;
    private Double lon;
    private Double importance;

    private String type;
    private String licence;
    private String osm_type;
    private String display_name;

    @JsonDeserialize
    private List< String > boundingbox;
    @JsonDeserialize
    private com.ssd.mvd.address.Data address;
}
