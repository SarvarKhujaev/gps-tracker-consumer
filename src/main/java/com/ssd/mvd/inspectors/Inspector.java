package com.ssd.mvd.inspectors;

import com.ssd.mvd.entity.ApiResponseModel;
import com.ssd.mvd.entity.TrackerInfo;
import com.ssd.mvd.entity.Status;
import com.ssd.mvd.entity.Icons;

import reactor.core.publisher.Mono;
import java.time.Duration;
import java.time.Instant;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.Optional;
import java.util.HashMap;
import java.util.Date;
import java.util.Map;

public class Inspector {
    // содержит все типы полицейских
    public final Map< String, Icons > icons = new HashMap<>();
    // хранит все не зарегистрированные трекеры
    protected final Map< String, Date > unregisteredTrackers = new HashMap<>();
    protected final Map< String, TrackerInfo > tupleOfCarMap = new HashMap<>();
    protected final Map< String, TrackerInfo > trackerInfoMap = new HashMap<>();

    protected <T> Mono< T > convert ( final T o ) { return Optional.ofNullable( o ).isPresent() ? Mono.just( o ) : Mono.empty(); }

    public final Supplier< Date > getDate = Date::new;

    public final BiFunction< Long, Instant, Long > getTimeDifference = ( aLong, instant ) ->
            Math.abs( aLong + Duration.between( Instant.now(), instant ).getSeconds() );

    protected final Function< Map< String, ? >, Mono< ApiResponseModel > > function =
            map -> this.convert( ApiResponseModel
                    .builder() // in case of wrong login
                    .status( Status
                            .builder()
                            .message( map.get( "message" ).toString() )
                            .code( map.containsKey( "code" ) ? Integer.parseInt( map.get( "code" ).toString() ) : 200 )
                            .build() )
                    .success( !map.containsKey( "success" ) )
                    .build() );

    protected final Supplier< Mono< ApiResponseModel > > errorResponse = () -> this.convert(
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
