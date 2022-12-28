package com.ssd.mvd.controller;

import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ssd.mvd.database.CassandraDataControlForEscort;
import com.ssd.mvd.entity.*;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.List;
import java.util.UUID;


@Slf4j
@RestController
public class CarForEscortController {
    private final Consumer< Throwable > error = throwable -> log.error( "Error: {}", throwable.getMessage() );
    private final Supplier< ApiResponseModel > errorResponse = () -> ApiResponseModel
            .builder()
            .success( false )
            .status( Status
                    .builder()
                    .message( "Server Error" )
                    .code( 201 )
                    .build() )
            .build();

    @MessageMapping ( value = "getAllCarsForEscort" )
    public Flux< TupleOfCar > getAllCarsForEscort() { return CassandraDataControlForEscort
            .getInstance()
            .getGetAllTupleOfCar()
            .get()
            .onErrorContinue( ( throwable, o ) -> this.error.accept( throwable ) ); }

    @MessageMapping ( value = "getAllTrackersIdForEscort" )
    public Flux< LastPosition > getAllTrackersId () { return CassandraDataControlForEscort
            .getInstance()
            .getGetAllTrackers()
            .get()
            .map( trackerInfo -> new LastPosition( trackerInfo, true ) )
            .onErrorContinue( ( throwable, o ) -> this.error.accept( throwable ) ); }

    @MessageMapping ( value = "getAllTrackersForEscortCar" )
    public Flux< TrackerInfo > getAllTrackersForEscortCar () { return CassandraDataControlForEscort
            .getInstance()
            .getGetAllTrackers()
            .get()
            .onErrorContinue( ( throwable, o ) -> this.error.accept( throwable ) ); }

    @MessageMapping ( value = "getCurrentTracker" )
    public Mono< TrackerInfo > getCurrentTracker ( String trackerId ) { return CassandraDataControlForEscort
            .getInstance()
            .getGetCurrentTracker()
            .apply( trackerId )
            .onErrorContinue( ( throwable, o ) -> this.error.accept( throwable ) ); }

    @MessageMapping ( value = "getCurrentForEscort" )
    public Mono< TupleOfCar > getCurrentForEscort( String gosNumber ) {
        return CassandraDataControlForEscort
            .getInstance()
            .getGetCurrentTupleofCar()
            .apply( UUID.fromString( gosNumber ) )
            .onErrorContinue( ( throwable, o ) -> this.error.accept( throwable ) ); }

    @MessageMapping ( value = "deleteCarForEscort" )
    public Mono< ApiResponseModel > deleteCarForEscort( String gosNumber ) { return CassandraDataControlForEscort
            .getInstance()
            .getDeleteTupleOfCar()
            .apply( gosNumber )
            .onErrorContinue( ( throwable, o ) -> this.error.accept( throwable ) )
            .onErrorReturn( this.errorResponse.get() ); }

    @MessageMapping ( value = "updateEscortCar" )
    public Mono< ApiResponseModel > updateEscortCar ( TupleOfCar tupleOfCar ) { return CassandraDataControlForEscort
            .getInstance()
            .getUpdateEscortCar()
            .apply( tupleOfCar )
            .onErrorContinue( ( throwable, o ) -> this.error.accept( throwable ) )
            .onErrorReturn( this.errorResponse.get() ); }

    @MessageMapping( value = "addNewCarForEscort" )
    public Mono< ApiResponseModel > addNewCarForEscort ( TupleOfCar tupleOfCar ) { return CassandraDataControlForEscort
            .getInstance()
            .getSaveNewTupleOfCar()
            .apply( tupleOfCar )
            .onErrorContinue( ( throwable, o ) -> this.error.accept( throwable ) )
            .onErrorReturn( this.errorResponse.get() ); }

    @MessageMapping ( value = "findTheClosestCarsInRadius" )
    public Flux< TupleOfCar > findTheClosestCarsInRadius ( Point point ) {
        return CassandraDataControlForEscort
                .getInstance()
                .getFindTheClosestCarsInRadius()
                .apply( point ); }

    @MessageMapping ( value = "findTheClosestCarsinPolygon" )
    public Flux< TupleOfCar > findTheClosestCarsinPolygon ( List< Point > pointList ) {
        return CassandraDataControlForEscort
                .getInstance()
                .getFindTheClosestCarsinPolygon()
                .apply( pointList ); }
}
