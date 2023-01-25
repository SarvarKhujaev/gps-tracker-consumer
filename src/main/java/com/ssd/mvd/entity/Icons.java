package com.ssd.mvd.entity;

import com.datastax.driver.core.Row;
import lombok.Data;

@Data
public class Icons {
    private String icon1;
    private String icon2;

    public Icons ( Row row ) {
        this.setIcon1( row != null ? row.getString( "icon" ) : null );
        this.setIcon2( row != null ? row.getString( "icon2" ) : null ); }
}