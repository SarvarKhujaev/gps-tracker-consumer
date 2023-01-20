package com.ssd.mvd.kafka;

import com.ssd.mvd.database.CassandraDataControlForEscort;
import com.ssd.mvd.database.CassandraDataControl;
import com.ssd.mvd.entity.ApiResponseModel;
import com.ssd.mvd.entity.TrackerInfo;
import com.ssd.mvd.entity.Status;

import java.util.function.Function;
import java.util.HashMap;
import java.util.Map;

import reactor.core.publisher.Mono;
import lombok.Data;

@Data
public class Inspector {
    private static Inspector inspector = new Inspector();
    private final Map< String, TrackerInfo > tupleOfCarMap = new HashMap<>();
    private final Map< String, TrackerInfo > trackerInfoMap = new HashMap<>();

    public static Inspector getInspector() { return inspector != null ? inspector : ( inspector = new Inspector() ); }

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

    private Inspector () {
        CassandraDataControl
                .getInstance()
                .getGetAllTrackers()
                .get()
                .subscribe( trackerInfo -> this.getTrackerInfoMap()
                        .putIfAbsent( trackerInfo.getTrackerId(), trackerInfo ) );

        CassandraDataControlForEscort
                .getInstance()
                .getGetAllTrackers()
                .get()
                .subscribe( trackerInfo -> this.getTupleOfCarMap()
                        .putIfAbsent( trackerInfo.getTrackerId(), trackerInfo ) ); }

    public void stop () {
        inspector = null;
        this.getTupleOfCarMap().clear();
        this.getTrackerInfoMap().clear(); } // stopping all consumers
}
