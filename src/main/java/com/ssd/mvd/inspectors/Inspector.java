package com.ssd.mvd.inspectors;

import com.ssd.mvd.annotations.EntityConstructorAnnotation;
import com.ssd.mvd.entity.ApiResponseModel;
import com.ssd.mvd.entity.TrackerInfo;
import com.ssd.mvd.entity.Status;
import com.ssd.mvd.entity.Icons;

import com.datastax.driver.core.GettableData;
import reactor.core.publisher.Mono;

import java.util.WeakHashMap;
import java.util.Date;
import java.util.Map;

@com.ssd.mvd.annotations.ImmutableEntityAnnotation
public class Inspector extends DataValidateInspector {
    protected Inspector () {
        icons = super.newMap();
        tupleOfCarMap = super.newMap();
        trackerInfoMap = super.newMap();
        unregisteredTrackers = super.newMap();
    }

    @EntityConstructorAnnotation( permission = WebFluxInspector.class )
    protected <T extends UuidInspector> Inspector ( @lombok.NonNull final Class<T> instance ) {
        super( Inspector.class );

        AnnotationInspector.checkCallerPermission( instance, Inspector.class );
        AnnotationInspector.checkAnnotationIsImmutable( Inspector.class );
    }

    // содержит все типы полицейских
    public static WeakHashMap< String, Icons > icons;
    // хранит все не зарегистрированные трекеры
    public static WeakHashMap< String, Date > unregisteredTrackers;
    public static WeakHashMap< String, TrackerInfo > tupleOfCarMap;
    public static WeakHashMap< String, TrackerInfo > trackerInfoMap;

    @lombok.Synchronized
    protected final synchronized void save (
            @lombok.NonNull final GettableData row
    ) {
        icons.put(
                row.getString( "policeType" ),
                EntitiesInstances.ICONS.get().generate().generate( row )
        );
    }

    @lombok.Synchronized
    protected final synchronized void save (
            @lombok.NonNull final TrackerInfo trackerInfo
    ) {
        trackerInfoMap.putIfAbsent( trackerInfo.getTrackerId(), trackerInfo );
    }

    protected final synchronized void saveTuple (
            @lombok.NonNull final TrackerInfo trackerInfo
    ) {
        tupleOfCarMap.putIfAbsent( trackerInfo.getTrackerId(), trackerInfo );
    }

    protected final synchronized <T> Mono< T > convert ( final T o ) {
        return Mono.justOrEmpty( o );
    }

    protected final synchronized Mono< ApiResponseModel > getResponse (
            @lombok.NonNull final Map< String, ? > map
    ) {
        return this.convert(
                ApiResponseModel
                        .builder()
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
