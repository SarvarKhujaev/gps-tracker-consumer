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


@RestController
public class CarForEscortController extends LogInspector {
    @MessageMapping ( value = "getAllCarsForEscort" )
    public Flux< TupleOfCar > getAllCarsForEscort() { return CassandraDataControlForEscort
            .getInstance()
            .getGetAllTupleOfCar()
            .get()
            .onErrorContinue( super::logging ); }

    @MessageMapping ( value = "getAllTrackersIdForEscort" )
    public Flux< LastPosition > getAllTrackersId () { return CassandraDataControlForEscort
            .getInstance()
            .getGetAllTrackers()
            .get()
            .map( trackerInfo -> new LastPosition( trackerInfo, trackerInfo.getPatrul() ) )
            .onErrorContinue( super::logging ); }

    @MessageMapping ( value = "getAllTrackersForEscortCar" )
    public Flux< TrackerInfo > getAllTrackersForEscortCar () { return CassandraDataControlForEscort
            .getInstance()
            .getGetAllTrackers()
            .get()
            .onErrorContinue( super::logging ); }

    @MessageMapping ( value = "getCurrentTracker" )
    public Mono< TrackerInfo > getCurrentTracker ( String trackerId ) {
        return CassandraDataControlForEscort
            .getInstance()
            .getGetCurrentTracker()
            .apply( trackerId )
            .onErrorContinue( super::logging ); }

    @MessageMapping ( value = "getCurrentForEscort" )
    public Mono< TupleOfCar > getCurrentForEscort ( String gosNumber ) {
        return CassandraDataControlForEscort
            .getInstance()
            .getGetCurrentTupleOfCar()
            .apply( UUID.fromString( gosNumber ) )
            .onErrorContinue( super::logging ); }

    @MessageMapping ( value = "findTheClosestCarsInRadius" )
    public Flux< TupleOfCar > findTheClosestCarsInRadius ( Point point ) {
        return CassandraDataControlForEscort
                .getInstance()
                .getFindTheClosestCarsInRadius()
                .apply( point )
                .onErrorContinue( super::logging ); }

    @MessageMapping ( value = "deleteCarForEscort" )
    public Mono< ApiResponseModel > deleteCarForEscort ( String gosNumber ) {
        return CassandraDataControlForEscort
                .getInstance()
                .getDeleteTupleOfCar()
                .apply( gosNumber )
                .onErrorResume( super::logging ); }

    @MessageMapping ( value = "updateEscortCar" )
    public Mono< ApiResponseModel > updateEscortCar ( TupleOfCar tupleOfCar ) {
        return CassandraDataControlForEscort
                .getInstance()
                .getUpdateEscortCar()
                .apply( tupleOfCar )
                .onErrorResume( super::logging ); }

    @MessageMapping( value = "addNewCarForEscort" )
    public Mono< ApiResponseModel > addNewCarForEscort ( TupleOfCar tupleOfCar ) {
        return CassandraDataControlForEscort
            .getInstance()
            .getSaveNewTupleOfCar()
            .apply( tupleOfCar )
            .onErrorContinue( super::logging ); }

    @MessageMapping ( value = "findTheClosestCarsinPolygon" )
    public Flux< TupleOfCar > findTheClosestCarsinPolygon ( List< Point > pointList ) {
        return CassandraDataControlForEscort
                .getInstance()
                .getFindTheClosestCarsinPolygon()
                .apply( pointList )
                .onErrorContinue( super::logging ); }
}
