package com.ssd.mvd.entity;

import com.datastax.driver.core.Row;
import com.ssd.mvd.inspectors.DataValidateInspector;

@lombok.Data
public class Icons {
    private String icon1;
    private String icon2;

    public Icons ( final Row row ) {
        this.setIcon1( DataValidateInspector
                .getInstance()
                .getCheckParam()
                .test( row )
                ? row.getString( "icon" ) : null );

        this.setIcon2( DataValidateInspector
                .getInstance()
                .getCheckParam()
                .test( row )
                ? row.getString( "icon2" ) : null ); }
}