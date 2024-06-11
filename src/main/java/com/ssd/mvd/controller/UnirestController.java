package com.ssd.mvd.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.gson.Gson;

import com.mashape.unirest.http.ObjectMapper;
import com.mashape.unirest.http.Unirest;

import com.ssd.mvd.inspectors.LogInspector;
import com.ssd.mvd.constants.Status;
import com.ssd.mvd.address.Address;

import java.util.function.BiFunction;
import java.util.List;

public final class UnirestController extends LogInspector {
    private final String ADDRESS_LOCATION_API = super.checkContextOrReturnDefaultValue(
            "variables.ADDRESS_LOCATION_API",
            Status.NOT_AVAILABLE.name()
    );

    private static final UnirestController unirestController = new UnirestController();

    private final Gson gson = new Gson();

    public static UnirestController getInstance () {
        return unirestController;
    }

    private UnirestController () {
        Unirest.setObjectMapper( new ObjectMapper() {
            private final com.fasterxml.jackson.databind.ObjectMapper objectMapper = new com.fasterxml.jackson.databind.ObjectMapper();

            @Override
            public String writeValue( Object o ) {
                try {
                    return this.objectMapper.writeValueAsString( o );
                } catch ( final JsonProcessingException e ) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public <T> T readValue( String s, Class<T> aClass ) {
                try {
                    return this.objectMapper.readValue( s, aClass );
                }
                catch ( final JsonProcessingException e ) {
                    throw new RuntimeException(e);
                }
            }
        } );
    }

    private <T> List<T> stringToArrayList (
            final String object,
            final Class< T[] > clazz
    ) {
        return super.convertArrayToList( this.gson.fromJson( object, clazz ) );
    }

    public final BiFunction< Double, Double, String > getAddressByLocation = ( latitude, longitude ) -> {
            try {
                return this.stringToArrayList(
                        Unirest.get(
                                String.join(
                                        "",
                                        this.ADDRESS_LOCATION_API,
                                        String.join(
                                                ",",
                                                latitude.toString(),
                                                longitude.toString()
                                        ),
                                        "&limit=5&format=json&addressdetails=1"
                                )
                            ).asJson()
                            .getBody()
                            .getArray()
                            .toString(),
                            Address[].class
                    ).get( 0 )
                    .getDisplay_name();
            }
            catch ( final Exception e ) {
                super.logging( e, "address not found" );
                return Status.NOT_AVAILABLE.name();
            }
    };
}
