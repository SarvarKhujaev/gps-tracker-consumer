package com.ssd.mvd.inspectors;

import com.google.gson.GsonBuilder;
import com.google.gson.Gson;

import java.util.List;

public class SerDes extends LogInspector {
    private final static Gson gson = new GsonBuilder()
            .excludeFieldsWithoutExposeAnnotation()
            .create();

    protected final synchronized Gson getGson () {
        return gson;
    }

    protected final synchronized <T> String serialize ( final T object ) {
        return this.getGson().toJson( object );
    }

    protected final synchronized <T> T deserialize (
            final String value,
            final Class<T> clazz
    ) {
        return this.getGson().fromJson( value, clazz );
    }

    protected  <T> List<T> stringToArrayList (
            final String object,
            final Class< T[] > clazz
    ) {
        return super.convertArrayToList( this.getGson().fromJson( object, clazz ) );
    }
}
