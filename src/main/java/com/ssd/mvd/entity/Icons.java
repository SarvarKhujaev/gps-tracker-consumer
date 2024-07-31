package com.ssd.mvd.entity;

import com.datastax.driver.core.Row;
import com.ssd.mvd.inspectors.DataValidateInspector;
import com.ssd.mvd.interfaces.ObjectFromRowConvertInterface;

public final class Icons
        extends DataValidateInspector
        implements ObjectFromRowConvertInterface< Icons > {
    public String getIcon1() {
        return this.icon1;
    }

    public void setIcon1 ( final String icon1 ) {
        this.icon1 = icon1;
    }

    public String getIcon2() {
        return this.icon2;
    }

    public void setIcon2 ( final String icon2 ) {
        this.icon2 = icon2;
    }

    private String icon1;
    private String icon2;

    public Icons () {}

    @Override
    public Icons generate( final Row row ) {
        super.checkAndSetParams(
                row,
                row1 -> {
                    this.setIcon1( row.getString( "icon" ) );
                    this.setIcon2( row.getString( "icon2" ) );
                }
        );

        return this;
    }

    @Override
    public Icons generate() {
        return new Icons();
    }
}