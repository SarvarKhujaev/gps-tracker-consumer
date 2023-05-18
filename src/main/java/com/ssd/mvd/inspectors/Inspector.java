package com.ssd.mvd.inspectors;

import com.ssd.mvd.entity.ApiResponseModel;
import com.ssd.mvd.entity.TrackerInfo;
import com.ssd.mvd.entity.Status;

import reactor.core.publisher.Mono;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.HashMap;
import java.util.Date;
import java.util.Map;

@lombok.Data
public class Inspector {
    // хранит все не зарегистрированные трекеры
    private final Map< String, Date > unregisteredTrackers = new HashMap<>();
    private final Map< String, TrackerInfo > tupleOfCarMap = new HashMap<>();
    private final Map< String, TrackerInfo > trackerInfoMap = new HashMap<>();

    protected <T> Mono< T > convert ( final T o ) { return Mono.just( o ); }

    private final Function< Map< String, ? >, Mono< ApiResponseModel > > function =
            map -> Mono.just( ApiResponseModel
                    .builder() // in case of wrong login
                    .status( Status
                            .builder()
                            .message( map.get( "message" ).toString() )
                            .code( map.containsKey( "code" ) ?
                                    Integer.parseInt( map.get( "code" ).toString() ) : 200 )
                            .build() )
                    .success( !map.containsKey( "success" ) )
                    .build() );

    private final Supplier< Mono< ApiResponseModel > > errorResponse = () -> Mono.just(
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
