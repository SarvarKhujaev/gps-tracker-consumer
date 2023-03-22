package com.ssd.mvd.inspectors;

import com.ssd.mvd.database.CassandraDataControl;
import com.ssd.mvd.constants.CassandraTables;
import com.datastax.driver.core.Row;
import com.ssd.mvd.entity.Position;
import com.ssd.mvd.entity.Request;
import com.ssd.mvd.entity.ReqCar;
import com.ssd.mvd.entity.Point;

import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.Objects;
import java.util.Date;
import java.util.List;

import static java.lang.Math.*;
import static java.lang.Math.cos;

@lombok.Data
public class DataValidateInspector extends Inspector {
    private final Date date = new Date( 1605006666774L );
    private final Predicate< Object > checkParam = Objects::nonNull;

    private final Predicate< String > checkTupleTrackerId = s -> super.getTupleOfCarMap().containsKey( s );

    private final Predicate< String > checkReqCarTrackerId = s -> super.getTrackerInfoMap().containsKey( s );

    private final Predicate< Position > checkPosition = position ->
            position.getLatitude() > 0
            && position.getSpeed() > 0
            && position.getLongitude() > 0
            && position.getDeviceTime().after( this.getDate() );

    private final Predicate< ReqCar > checkReqCar = reqCar ->
            reqCar != null
            && reqCar.getPatrulPassportSeries() != null
            && reqCar.getPatrulPassportSeries().length() > 1
            && reqCar.getPatrulPassportSeries().compareTo( "null" ) != 0;

    private final Predicate< Row > checkTrackerTime = row -> Math.abs( row.getDouble( "totalActivityTime" ) ) > 0;

    private final Predicate< Request > checkRequest = request -> request.getStartTime() == null && request.getEndTime() == null;

    private final BiFunction< List< Point >, Row, Boolean > calculateDistanceInSquare = ( pointList, row ) -> {
        Boolean result = false;
        int j = pointList.size() - 1;
        for ( int i = 0; i < pointList.size(); i++ ) {
            if ( ( pointList.get( i ).getLatitude() < row.getDouble( "latitude" )
                    && pointList.get( j ).getLatitude() >= row.getDouble( "latitude" )
                    || pointList.get( j ).getLatitude() < row.getDouble( "latitude" )
                    && pointList.get( i ).getLatitude() >= row.getDouble( "latitude" ) )
                    && ( pointList.get( i ).getLongitude() + ( row.getDouble( "latitude" )
                    - pointList.get( i ).getLatitude() ) / ( pointList.get( j ).getLatitude() - pointList.get( j ).getLongitude() )
                    * ( pointList.get( j ).getLatitude() - pointList.get( i ).getLatitude() ) < row.getDouble( "longitude" ) ) )
                result = !result;
            j = i; }
        return result; };

    private static final Double p = PI / 180;

    private final BiFunction< Point, Row, Double > calculate = ( first, second ) ->
            12742 * asin( sqrt( 0.5 - cos( ( second.getDouble( "latitude" ) - first.getLatitude() ) * p ) / 2
                    + cos( first.getLatitude() * p ) * cos( second.getDouble( "latitude" ) * p )
                    * ( 1 - cos( ( second.getDouble( "longitude" ) - first.getLongitude() ) * p ) ) / 2 ) ) * 1000;

    private final Predicate< String > checkTracker = trackerId -> !this.getCheckParam()
            .test( CassandraDataControl
                    .getInstance()
                    .getSession()
                    .execute ( "SELECT * FROM "
                            + CassandraTables.ESCORT + "."
                            + CassandraTables.TRACKERSID
                            + " WHERE trackersId = '" + trackerId + "';" ).one() )
            && !this.getCheckParam().test( CassandraDataControl
            .getInstance()
            .getSession()
            .execute( "SELECT * FROM "
            + CassandraTables.TRACKERS + "."
            + CassandraTables.TRACKERSID
            + " WHERE trackersId = '" + trackerId + "';" ).one() );

    private final Predicate< String > checkCarNumber = carNumber ->
            !this.getCheckParam().test(
                    CassandraDataControl
                    .getInstance()
                    .getSession()
                    .execute( "SELECT * FROM "
                            + CassandraTables.ESCORT.name() + "."
                            + CassandraTables.TUPLE_OF_CAR.name() +
                            " WHERE gosnumber = '" + carNumber + "';" ).one() )
            && !this.getCheckParam().test(
                    CassandraDataControl
                    .getInstance()
                    .getSession()
                    .execute( "SELECT * FROM "
                            + CassandraTables.TABLETS.name() + "."
                            + CassandraTables.CARS.name()
                            + " WHERE gosnumber = '" + carNumber + "';" ).one() );
}
