package com.ssd.mvd.controller;

import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ssd.mvd.database.CassandraDataControlForEscort;
import com.ssd.mvd.inspectors.LogInspector;
import com.ssd.mvd.entity.*;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.UUID;
import java.util.Map;


@RestController
public class CarForEscortController extends LogInspector {
    @MessageMapping ( value = "getAllCarsForEscort" )
    public Flux< TupleOfCar > getAllCarsForEscort() { return CassandraDataControlForEscort
            .getInstance()
            .getGetAllTupleOfCar()
            .get()
            .onErrorContinue( super::logging ); }

    @MessageMapping ( value = "getAllTrackersForEscortCar" )
    public Flux< TrackerInfo > getAllTrackersForEscortCar () { return CassandraDataControlForEscort
            .getInstance()
            .getGetAllTrackers()
            .get()
            .onErrorContinue( super::logging ); }

    @MessageMapping ( value = "getCurrentTracker" )
    public Mono< TrackerInfo > getCurrentTracker ( final String trackerId ) {
        return CassandraDataControlForEscort
            .getInstance()
            .getGetCurrentTracker()
            .apply( trackerId )
            .onErrorContinue( super::logging ); }

    @MessageMapping ( value = "getCurrentForEscort" )
    public Mono< TupleOfCar > getCurrentForEscort ( final String gosNumber ) {
        return CassandraDataControlForEscort
            .getInstance()
            .getGetCurrentTupleOfCar()
            .apply( UUID.fromString( gosNumber ) )
            .onErrorContinue( super::logging ); }

    @MessageMapping ( value = "findTheClosestCarsInRadius" )
    public Flux< TupleOfCar > findTheClosestCarsInRadius ( final Point point ) {
            return super.getCheck().apply( point, 8 )
                    ? CassandraDataControlForEscort
                    .getInstance()
                    .getFindTheClosestCarsInRadius()
                    .apply( point )
                    .onErrorContinue( super::logging )
                    : Flux.empty(); }

    @MessageMapping ( value = "deleteCarForEscort" )
    public Mono< ApiResponseModel > deleteCarForEscort ( final String gosNumber ) {
            return CassandraDataControlForEscort
                    .getInstance()
                    .getDeleteTupleOfCar()
                    .apply( gosNumber )
                    .onErrorResume( super::logging ); }

    @MessageMapping ( value = "updateEscortCar" )
    public Mono< ApiResponseModel > updateEscortCar ( final TupleOfCar tupleOfCar ) {
            return CassandraDataControlForEscort
                    .getInstance()
                    .getUpdateEscortCar()
                    .apply( tupleOfCar )
                    .onErrorResume( super::logging ); }

    @MessageMapping ( value = "getAllTrackersIdForEscort" )
    public Flux< LastPosition > getAllTrackersId ( final Map< String, Long > params ) { return CassandraDataControlForEscort
            .getInstance()
            .getGetAllTrackers()
            .get()
            .filter( trackerInfo -> super.getCheckParam().test( params )
                    && params.size() > 0
                    && super.getCheckParam().test( trackerInfo.getPatrul() )
                    ? super.getCheckParams().apply( trackerInfo.getPatrul(), params )
                    : true )
            .map( trackerInfo -> new LastPosition( trackerInfo, trackerInfo.getPatrul() ) )
            .onErrorContinue( super::logging ); }

    @MessageMapping( value = "addNewCarForEscort" )
    public Mono< ApiResponseModel > addNewCarForEscort ( final TupleOfCar tupleOfCar ) {
        return CassandraDataControlForEscort
            .getInstance()
            .getSaveNewTupleOfCar()
            .apply( tupleOfCar )
            .onErrorContinue( super::logging ); }

    @MessageMapping ( value = "findTheClosestCarsinPolygon" )
    public Flux< TupleOfCar > findTheClosestCarsinPolygon ( final List< Point > pointList ) {
            return super.getCheckParam().test( pointList )
                    && pointList.size() > 0
                    ? CassandraDataControlForEscort
                    .getInstance()
                    .getFindTheClosestCarsinPolygon()
                    .apply( pointList )
                    .onErrorContinue( super::logging )
                    : Flux.empty(); }
}
