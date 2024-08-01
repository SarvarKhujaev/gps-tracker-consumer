package com.ssd.mvd.controller;

import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ssd.mvd.database.CassandraDataControlForEscort;
import com.ssd.mvd.database.CassandraDataControl;
import com.ssd.mvd.inspectors.EntitiesInstances;
import com.ssd.mvd.inspectors.LogInspector;
import com.ssd.mvd.entity.*;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

@RestController
public final class CarForEscortController extends LogInspector {
    @MessageMapping ( value = "getAllCarsForEscort" )
    public Flux< TupleOfCar > getAllCarsForEscort() {
        return CassandraDataControl
                .getInstance()
                .getConvertedEntities( EntitiesInstances.TUPLE_OF_CAR )
                .onErrorContinue( super::logging );
    }

    @MessageMapping ( value = "getAllTrackersForEscortCar" )
    public Flux< TrackerInfo > getAllTrackersForEscortCar () {
        return CassandraDataControlForEscort
                .getInstance()
                .getAllTrackers
                .get()
                .onErrorContinue( super::logging );
    }

    @MessageMapping ( value = "getCurrentTracker" )
    public Mono< TrackerInfo > getCurrentTracker ( final String trackerId ) {
        return CassandraDataControlForEscort
                .getInstance()
                .getCurrentTracker
                .apply( trackerId )
                .onErrorContinue( super::logging );
    }

    @MessageMapping ( value = "getCurrentForEscort" )
    public Mono< TupleOfCar > getCurrentForEscort ( final String gosNumber ) {
        return super.convert(
                EntitiesInstances.TUPLE_OF_CAR.generate().generate(
                        CassandraDataControlForEscort
                                .getInstance()
                                .getRowFromTabletsKeyspace(
                                        EntitiesInstances.TUPLE_OF_CAR,
                                        "uuid",
                                        gosNumber
                                )
                )
        ).onErrorContinue( super::logging );
    }

    @MessageMapping ( value = "findTheClosestCarsInRadius" )
    public Flux< TupleOfCar > findTheClosestCarsInRadius ( final Point point ) {
            return super.objectIsNotNull( point )
                    ? CassandraDataControlForEscort
                            .getInstance()
                            .findTheClosestCarsInRadius
                            .apply( point )
                            .onErrorContinue( super::logging )
                    : Flux.empty();
    }

    @MessageMapping ( value = "deleteCarForEscort" )
    public Mono< ApiResponseModel > deleteCarForEscort ( final String gosNumber ) {
        return CassandraDataControlForEscort
                .getInstance()
                .deleteTupleOfCar
                .apply( gosNumber )
                .onErrorResume( super::logging );
    }

    @MessageMapping ( value = "updateEscortCar" )
    public Mono< ApiResponseModel > updateEscortCar ( final TupleOfCar tupleOfCar ) {
        return CassandraDataControlForEscort
                .getInstance()
                .updateEscortCar
                .apply( tupleOfCar )
                .onErrorResume( super::logging );
    }

    @MessageMapping ( value = "getAllTrackersIdForEscort" )
    public Flux< LastPosition > getAllTrackersId ( final Map< String, Long > params ) {
        return CassandraDataControlForEscort
            .getInstance()
            .getAllTrackers
            .get()
            .filter(
                    trackerInfo -> !super.objectIsNotNull( params )
                            || params.isEmpty()
                            || !super.objectIsNotNull( trackerInfo.getPatrul() )
                            || super.checkParams( trackerInfo.getPatrul(), params )
            ).map( trackerInfo -> new LastPosition( trackerInfo, trackerInfo.getPatrul() ) )
            .onErrorContinue( super::logging );
    }

    @MessageMapping( value = "addNewCarForEscort" )
    public Mono< ApiResponseModel > addNewCarForEscort ( final TupleOfCar tupleOfCar ) {
        return CassandraDataControlForEscort
                .getInstance()
                .saveNewTupleOfCar
                .apply( tupleOfCar )
                .onErrorContinue( super::logging );
    }

    @MessageMapping ( value = "findTheClosestCarsinPolygon" )
    public Flux< TupleOfCar > findTheClosestCarsinPolygon ( final List< Point > pointList ) {
            return super.isCollectionNotEmpty( pointList )
                    ? CassandraDataControlForEscort
                            .getInstance()
                            .findTheClosestCarsInPolygon
                            .apply( pointList )
                            .onErrorContinue( super::logging )
                    : Flux.empty();
    }
}
