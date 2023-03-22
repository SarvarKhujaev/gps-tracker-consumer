package com.ssd.mvd.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.gson.Gson;

import com.mashape.unirest.http.ObjectMapper;
import com.mashape.unirest.http.Unirest;

import com.ssd.mvd.GpsTrackerApplication;
import com.ssd.mvd.address.Address;

import lombok.extern.slf4j.Slf4j;
import lombok.Data;

import java.util.function.BiFunction;
import java.util.Arrays;
import java.util.List;

@Data
@Slf4j
public class UnirestController {
    private final String ADDRESS_LOCATION_API = GpsTrackerApplication
            .context
            .getEnvironment()
            .getProperty( "variables.ADDRESS_LOCATION_API" );

    private static UnirestController serDes = new UnirestController();

    private final Gson gson = new Gson();

    public static UnirestController getInstance () { return serDes != null ? serDes : ( serDes = new UnirestController() ); }

    private UnirestController() {
        Unirest.setObjectMapper( new ObjectMapper() {
            private final com.fasterxml.jackson.databind.ObjectMapper objectMapper = new com.fasterxml.jackson.databind.ObjectMapper();

            @Override
            public String writeValue( Object o ) {
                try { return this.objectMapper.writeValueAsString( o ); }
                catch ( JsonProcessingException e ) { throw new RuntimeException(e); } }

            @Override
            public <T> T readValue( String s, Class<T> aClass ) {
                try { return this.objectMapper.readValue( s, aClass ); }
                catch ( JsonProcessingException e ) { throw new RuntimeException(e); } } } ); }

    private <T> List<T> stringToArrayList ( String object, Class< T[] > clazz ) { return Arrays.asList( this.getGson().fromJson( object, clazz ) ); }

    private final BiFunction< Double, Double, String > getAddressByLocation = ( latitude, longitude ) -> {
        try { return this.stringToArrayList(
                    Unirest.get( this.getADDRESS_LOCATION_API()
                            + latitude + "," + longitude
                            + "&limit=5&format=json&addressdetails=1" )
                            .asJson()
                            .getBody()
                            .getArray()
                            .toString(),
                    Address[].class )
            .get( 0 )
            .getDisplay_name();
        } catch ( Exception e ) { return "address not found"; } };
}
