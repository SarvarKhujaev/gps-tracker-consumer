package com.ssd.mvd.inspectors;

import com.ssd.mvd.entity.ApiResponseModel;
import com.ssd.mvd.entity.TrackerInfo;
import com.ssd.mvd.entity.Status;
import com.ssd.mvd.entity.Icons;

import reactor.core.publisher.Mono;
import java.util.Optional;
import java.util.Date;
import java.util.Map;

public class Inspector extends CollectionsInspector {
    protected Inspector () {
        this.icons = super.newMap();
        this.tupleOfCarMap = super.newMap();
        this.trackerInfoMap = super.newMap();
        this.unregisteredTrackers = super.newMap();
    }

    // содержит все типы полицейских
    protected final Map< String, Icons > icons;
    // хранит все не зарегистрированные трекеры
    protected final Map< String, Date > unregisteredTrackers;
    protected final Map< String, TrackerInfo > tupleOfCarMap;
    protected final Map< String, TrackerInfo > trackerInfoMap;

    protected <T> Mono< T > convert ( final T o ) {
        return Optional.ofNullable( o ).isPresent() ? Mono.just( o ) : Mono.empty();
    }

    protected Mono< ApiResponseModel > getResponse (
            final Map< String, ? > map
    ) {
        return this.convert( ApiResponseModel
                .builder() // in case of wrong login
                .status( Status
                        .builder()
                        .message( map.get( "message" ).toString() )
                        .code( map.containsKey( "code" )
                                ? Integer.parseInt( map.get( "code" ).toString() )
                                : 200 )
                        .build() )
                .success( !map.containsKey( "success" ) )
                .build() );
    }

    protected Mono< ApiResponseModel > getErrorResponse () {
        return this.convert(
                ApiResponseModel
                        .builder()
                        .success( false )
                        .status( Status
                                .builder()
                                .message( "Server Error" )
                                .code( 201 )
                                .build() )
                        .build() );
    }
}
