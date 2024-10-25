package com.ssd.mvd.entity.patrulDataSet.patrulSubClasses;

import com.datastax.driver.core.GettableData;
import com.ssd.mvd.inspectors.DataValidateInspector;
import com.ssd.mvd.interfaces.ObjectFromRowConvertInterface;

public final class PatrulRegionData implements ObjectFromRowConvertInterface< PatrulRegionData > {
    public long getRegionId() {
        return this.regionId;
    }

    public void setRegionId( final long regionId ) {
        this.regionId = regionId;
    }

    public long getMahallaId() {
        return this.mahallaId;
    }

    public void setMahallaId( final long mahallaId ) {
        this.mahallaId = mahallaId;
    }

    public long getDistrictId() {
        return this.districtId;
    }

    public void setDistrictId( final long districtId ) {
        this.districtId = districtId;
    }

    public String getRegionName() {
        return this.regionName;
    }

    public void setRegionName( final String regionName ) {
        this.regionName = regionName;
    }

    public String getDistrictName() {
        return this.districtName;
    }

    public void setDistrictName( final String districtName ) {
        this.districtName = districtName;
    }

    private long regionId;
    private long mahallaId;
    private long districtId; // choosing from dictionary

    private String regionName;
    private String districtName;

    public PatrulRegionData () {}

    @Override
    @lombok.NonNull
    public PatrulRegionData generate() {
        return new PatrulRegionData();
    }

    @Override
    @lombok.NonNull
    public PatrulRegionData generate( @lombok.NonNull final GettableData udtValue ) {
        DataValidateInspector.checkAndSetParams(
                udtValue,
                udtValue1 -> {
                    this.setRegionId( udtValue.getLong( "regionId" ) );
                    this.setMahallaId( udtValue.getLong( "mahallaId" ) );
                    this.setDistrictId( udtValue.getLong( "districtId" ) );

                    this.setRegionName( udtValue.getString( "regionName" ) );
                    this.setDistrictName( udtValue.getString( "districtName" ) );
                }
        );

        return this;
    }
}
