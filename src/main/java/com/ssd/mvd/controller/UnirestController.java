package com.ssd.mvd.controller;

import com.fasterxml.jackson.core.JsonProcessingException;

import com.mashape.unirest.http.ObjectMapper;
import com.mashape.unirest.http.Unirest;

import com.ssd.mvd.inspectors.SerDes;
import com.ssd.mvd.constants.Errors;
import com.ssd.mvd.address.Address;

import java.util.function.BiFunction;

public final class UnirestController extends SerDes {
    private static final UnirestController UNIREST_CONTROLLER = new UnirestController();

    public static UnirestController getInstance () {
        return UNIREST_CONTROLLER;
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

    public final BiFunction< Double, Double, String > getAddressByLocation = ( latitude, longitude ) -> {
            try {
                return super.stringToArrayList(
                        Unirest.get(
                                String.join(
                                        "",
                                        super.checkContextOrReturnDefaultValue(
                                                "variables.ADDRESS_LOCATION_API",
                                                Errors.DATA_NOT_FOUND.name()
                                        ),
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
                super.logging( e, Errors.DATA_NOT_FOUND.name() );
                return Errors.DATA_NOT_FOUND.name();
            }
    };
}
