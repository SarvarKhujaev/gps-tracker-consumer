package com.ssd.mvd.inspectors;

import com.ssd.mvd.database.CassandraDataControl;
import com.ssd.mvd.constants.CassandraTables;
import com.datastax.driver.core.Row;
import com.ssd.mvd.entity.*;

import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Function;
import java.util.Objects;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static java.lang.Math.cos;
import static java.lang.Math.*;

@lombok.Data
public class DataValidateInspector extends Inspector {
    private static final Double P = PI / 180;
    private final Date date = new Date( 1605006666774L );
    private final static DataValidateInspector INSPECTOR = new DataValidateInspector();

    public static DataValidateInspector getInstance () { return INSPECTOR; }

    private final Predicate< Object > checkParam = Objects::nonNull;

    private final BiFunction< Object, Integer, Boolean > check = ( o, integer ) -> switch ( integer ) {
            case 1 -> super.getTupleOfCarMap().containsKey( String.valueOf( o ) );
            case 2 -> super.getTrackerInfoMap().containsKey( String.valueOf( o ) );
            case 4 -> ( (Row) o ).getDouble( "longitude" ) > 0 && ( (Row) o ).getDouble( "latitude" ) > 0;
            case 5 -> ( (Request) o ).getStartTime() == null && ( (Request) o ).getEndTime() == null;
            case 6 -> ( (Position) o ).getLatitude() > 0
                    && ( (Position) o ).getSpeed() > 0
                    && ( (Position) o ).getLongitude() > 0
                    && ( (Position) o ).getDeviceTime().after( this.getDate() );
            case 7 -> ( (Request) o ).getTrackerId() != null
                    && ( (Request) o ).getEndTime() != null
                    && ( (Request) o ).getStartTime() != null;
            case 8 -> o != null
                    && ( (Point) o ).getLatitude() != null
                    && ( (Point) o ).getLongitude() != null;
            default -> o != null
                    && ( (ReqCar) o ).getPatrulPassportSeries() != null
                    && ( (ReqCar) o ).getPatrulPassportSeries().length() > 1
                    && ( (ReqCar) o ).getPatrulPassportSeries().compareTo( "null" ) != 0; };

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

    private final Function< Integer, Integer > checkDifference = integer -> integer > 0 && integer < 100 ? integer : 10;

    private final BiFunction< Point, Row, Double > calculate = ( first, second ) ->
            12742 * asin( sqrt( 0.5 - cos( ( second.getDouble( "latitude" ) - first.getLatitude() ) * P) / 2
                    + cos( first.getLatitude() * P) * cos( second.getDouble( "latitude" ) * P)
                    * ( 1 - cos( ( second.getDouble( "longitude" ) - first.getLongitude() ) * P) ) / 2 ) ) * 1000;

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

    private final BiFunction< Patrul, Map< String, Long >, Boolean > checkParams = ( patrul, params ) -> switch ( params.size() ) {
            case 1 -> params.containsKey( "viloyat" ) && Objects.equals( patrul.getRegionId(), params.get( "viloyat" ) );
            case 2 -> ( params.containsKey( "viloyat" ) && Objects.equals( patrul.getRegionId(), params.get( "viloyat" ) ) )
                    && ( params.containsKey( "tuman" ) && Objects.equals( patrul.getDistrictId(), params.get( "tuman" ) ) );
            default -> ( params.containsKey( "tuman" ) && Objects.equals( patrul.getDistrictId(), params.get( "tuman" ) ) )
                    && ( params.containsKey( "viloyat" ) && Objects.equals( patrul.getRegionId(), params.get( "viloyat" ) ) )
                    && ( params.containsKey( "mahalla" ) && Objects.equals( patrul.getMahallaId(), params.get( "mahalla" ) ) ); };
}
