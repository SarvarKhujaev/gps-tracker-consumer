package com.ssd.mvd.entity;

import com.datastax.driver.core.Row;
import lombok.Data;

@Data
public class Icons {
    private String icon1;
    private String icon2;

    public Icons ( Row row ) {
        if ( row != null ) {
            this.setIcon1( row.getString( "icon" ) );
            this.setIcon2( row.getString( "icon2" ) ); }
        else {
            this.setIcon1( row.getString( "not found" ) );
            this.setIcon2( row.getString( "not found" ) ); } }
}
