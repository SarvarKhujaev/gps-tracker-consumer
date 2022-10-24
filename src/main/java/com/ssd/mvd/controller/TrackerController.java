package com.ssd.mvd.controller;

import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ssd.mvd.database.CassandraDataControl;
import com.ssd.mvd.entity.*;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
public class TrackerController {
    @MessageMapping ( value = "getAllTrackers" )
    public Flux< TrackerInfo > getAllTrackers () { return CassandraDataControl
            .getInstance()
            .getAllTrackers
            .get(); }

    @MessageMapping( value = "online" )
    public Flux< TrackerInfo > online () { return CassandraDataControl.getInstance()
            .getAllTrackers
            .get()
            .filter( TrackerInfo::getStatus ); }

    @MessageMapping( value = "offline" )
    public Flux< TrackerInfo > offline () { return CassandraDataControl
            .getInstance()
            .getAllTrackers
            .get()
            .filter( trackerInfo -> !trackerInfo.getStatus() ); }

    @MessageMapping ( value = "getAllTrackersId" )
    public Flux< LastPosition > getAllTrackersId () { return CassandraDataControl
            .getInstance()
            .getAllTrackers
            .get()
            .map( LastPosition::new ); }

    @MessageMapping ( "getTrackerHistory" )
    public Flux< PositionInfo > getTrackerHistory ( Request request ) { return CassandraDataControl
            .getInstance()
            .getHistoricalPosition
            .apply( request ); }

    @MessageMapping ( value = "ping" )
    public Mono< Boolean > ping () { return Mono.just( true ); }

    @MessageMapping ( value = "calculate_average_fuel_consumption" )
    public Mono< PatrulFuelStatistics > calculate_average_fuel_consumption ( Request request ) {
        return CassandraDataControl
            .getInstance()
            .getCalculate_average_fuel_consumption()
            .apply( request ); }
}
