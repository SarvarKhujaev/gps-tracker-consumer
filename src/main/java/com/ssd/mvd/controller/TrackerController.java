package com.ssd.mvd.controller;

import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ssd.mvd.database.CassandraDataControl;
import com.ssd.mvd.inspectors.Inspector;
import com.ssd.mvd.entity.*;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Date;
import java.util.Map;

@RestController
public class TrackerController extends Inspector {
    @MessageMapping ( value = "PING" )
    public Mono< Boolean > ping () { return Mono.just( true ); }

    @MessageMapping( value = "ONLINE" )
    public Flux< TrackerInfo > online () { return CassandraDataControl
            .getInstance()
            .getGetAllTrackers()
            .get()
            .filter( TrackerInfo::getStatus ); }

    @MessageMapping( value = "OFFLINE" )
    public Flux< TrackerInfo > offline () { return CassandraDataControl
            .getInstance()
            .getGetAllTrackers()
            .get()
            .filter( trackerInfo -> !trackerInfo.getStatus() ); }

    @MessageMapping ( value = "GET_ALL_TRACKERS" )
    public Flux< TrackerInfo > getAllTrackers () { return CassandraDataControl
            .getInstance()
            .getGetAllTrackers()
            .get(); }

    @MessageMapping ( value = "GET_ALL_TRACKERS_ID" )
    public Flux< LastPosition > getAllTrackersId () { return CassandraDataControl
            .getInstance()
            .getGetAllTrackers()
            .get()
            .map( LastPosition::new ); }

    @MessageMapping ( "GET_TRACKER_HISTORY" )
    public Flux< PositionInfo > getTrackerHistory ( Request request ) { return CassandraDataControl
            .getInstance()
            .getGetHistoricalPosition()
            .apply( request ); }

    @MessageMapping ( value = "GET_ALL_UNREGISTERED_TRACKERS" )
    public Mono< Map< String, Date > > GET_ALL_UNREGISTERED_TRACKERS () { return Mono.just( super.getUnregisteredTrackers() ); }

    @MessageMapping ( value = "CALCULATE_AVERAGE_FUEL_CONSUMPTION" )
    public Mono< PatrulFuelStatistics > calculate_average_fuel_consumption ( Request request ) {
        return CassandraDataControl
            .getInstance()
            .getCalculate_average_fuel_consumption()
            .apply( request ); }
}
