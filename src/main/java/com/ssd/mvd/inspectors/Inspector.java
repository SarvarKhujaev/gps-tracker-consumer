package com.ssd.mvd.inspectors;

import com.ssd.mvd.entity.ApiResponseModel;
import com.ssd.mvd.entity.TrackerInfo;
import com.ssd.mvd.entity.Status;
import com.ssd.mvd.entity.Icons;

import com.datastax.driver.core.Row;
import reactor.core.publisher.Mono;

import java.util.Date;
import java.util.Map;

public class Inspector extends DataValidateInspector {
    protected Inspector () {
        icons = super.newMap();
        tupleOfCarMap = super.newMap();
        trackerInfoMap = super.newMap();
        unregisteredTrackers = super.newMap();
    }

    // содержит все типы полицейских
    public static Map< String, Icons > icons;
    // хранит все не зарегистрированные трекеры
    public static Map< String, Date > unregisteredTrackers;
    public static Map< String, TrackerInfo > tupleOfCarMap;
    public static Map< String, TrackerInfo > trackerInfoMap;

    protected final synchronized void save (
            final Row row
    ) {
        icons.put( row.getString( "policeType" ), new Icons().generate( row ) );
    }

    protected final synchronized void save (
            final TrackerInfo trackerInfo
    ) {
        trackerInfoMap.putIfAbsent( trackerInfo.getTrackerId(), trackerInfo );
    }

    protected final synchronized void saveTuple (
            final TrackerInfo trackerInfo
    ) {
        tupleOfCarMap.putIfAbsent( trackerInfo.getTrackerId(), trackerInfo );
    }

    protected final synchronized  <T> Mono< T > convert ( final T o ) {
        return super.objectIsNotNull( o ) ? Mono.just( o ) : Mono.empty();
    }

    protected final synchronized Mono< ApiResponseModel > getResponse (
            final Map< String, ? > map
    ) {
        return this.convert(
                ApiResponseModel
                        .builder() // in case of wrong login
                        .status(
                                Status
                                    .builder()
                                    .message( map.get( "message" ).toString() )
                                    .code(
                                            map.containsKey( "code" )
                                                    ? Integer.parseInt( map.get( "code" ).toString() )
                                                    : 200
                                    ).build()
                        ).success( !map.containsKey( "success" ) )
                        .build()
        );
    }

    protected final synchronized Mono< ApiResponseModel > getErrorResponse () {
        return this.convert(
                ApiResponseModel
                        .builder()
                        .success( false )
                        .status(
                                Status
                                    .builder()
                                    .message( "Server Error" )
                                    .code( 201 )
                                    .build()
                        ).build()
        );
    }
}
