package com.ssd.mvd.controller;

import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ssd.mvd.database.CassandraDataControl;
import com.ssd.mvd.inspectors.Inspector;
import com.ssd.mvd.entity.*;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Comparator;
import java.util.Date;
import java.util.Map;

@RestController
public class TrackerController extends Inspector {
    @MessageMapping ( value = "PING" )
    public Mono< Boolean > ping () { return Mono.just( true ); }

    @MessageMapping ( value = "GET_ADDRESS" )
    public Mono< String > getAddress ( Point point ) {
        return point != null
                && point.getLatitude() != null
                && point.getLongitude() != null
                ? Mono.just( UnirestController
                .getInstance()
                .getGetAddressByLocation()
                .apply( point.getLatitude(), point.getLongitude() ) )
                : Mono.empty(); }

    @MessageMapping( value = "ONLINE" )
    public Flux< TrackerInfo > online () { return CassandraDataControl
            .getInstance()
            .getGetAllTrackers()
            .apply( true )
            .filter( TrackerInfo::getStatus ); }

    @MessageMapping( value = "OFFLINE" )
    public Flux< TrackerInfo > offline () { return CassandraDataControl
            .getInstance()
            .getGetAllTrackers()
            .apply( true )
            .filter( trackerInfo -> !trackerInfo.getStatus() ); }

    @MessageMapping ( value = "GET_ALL_TRACKERS" )
    public Flux< TrackerInfo > getAllTrackers () { return CassandraDataControl
            .getInstance()
            .getGetAllTrackers()
            .apply( true ); }

    @MessageMapping ( value = "GET_ALL_TRACKERS_ID" )
    public Flux< LastPosition > getAllTrackersId () { return CassandraDataControl
            .getInstance()
            .getGetAllTrackers()
            .apply( true )
            .map( LastPosition::new ); }

    @MessageMapping ( "GET_TRACKER_HISTORY" )
    public Flux< PositionInfo > getTrackerHistory ( Request request ) { return CassandraDataControl
            .getInstance()
            .getGetHistoricalPosition()
            .apply( request )
            .sort( Comparator.comparing( PositionInfo::getPositionWasSavedDate ) ); }

    @MessageMapping ( value = "GET_ALL_UNREGISTERED_TRACKERS" )
    public Mono< Map< String, Date > > GET_ALL_UNREGISTERED_TRACKERS () { return Mono.just( super.getUnregisteredTrackers() ); }

    @MessageMapping ( value = "CALCULATE_AVERAGE_FUEL_CONSUMPTION" )
    public Mono< PatrulFuelStatistics > calculate_average_fuel_consumption ( Request request ) {
        return CassandraDataControl
            .getInstance()
            .getCalculate_average_fuel_consumption()
            .apply( request ); }
}
