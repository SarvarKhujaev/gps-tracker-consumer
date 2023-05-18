package com.ssd.mvd.entity;

import com.datastax.driver.core.Row;
import com.ssd.mvd.inspectors.DataValidateInspector;

@lombok.Data
public class Icons {
    private String icon1;
    private String icon2;

    public Icons ( final Row row ) {
        if ( DataValidateInspector
                .getInstance()
                .checkParam
                .test( row ) ) {
            this.setIcon1( row.getString( "icon" ) );
            this.setIcon2( row.getString( "icon2" ) ); } }
}