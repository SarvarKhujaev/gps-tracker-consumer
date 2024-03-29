package com.ssd.mvd.inspectors;

import com.ssd.mvd.database.CassandraDataControlForEscort;
import com.ssd.mvd.database.CassandraDataControl;
import com.ssd.mvd.entity.patrulDataSet.Patrul;
import com.ssd.mvd.constants.CassandraTables;
import com.ssd.mvd.entity.*;

import com.datastax.driver.core.Row;
import static java.lang.Math.cos;
import static java.lang.Math.*;

import java.util.Objects;
import java.util.List;
import java.util.Map;

public class DataValidateInspector extends TimeInspector {
    protected DataValidateInspector () {}

    private static final Double P = PI / 180;
    private final static DataValidateInspector INSPECTOR = new DataValidateInspector();

    public static DataValidateInspector getInstance () {
        return INSPECTOR;
    }

    protected <T> boolean objectIsNotNull ( final T value ) {
        return Objects.nonNull( value );
    }

    protected boolean check (
            final String trackerId
    ) {
        return super.tupleOfCarMap.containsKey( trackerId )
                && super.trackerInfoMap.containsKey( trackerId );
    }

    protected boolean check (
            final Row row
    ) {
        return row.getDouble( "longitude" ) > 0 && row.getDouble( "latitude" ) > 0;
    }

    protected boolean check (
            final Request request
    ) {
        return request.getStartTime() == null && request.getEndTime() == null;
    }

    protected boolean check (
            final Position position
    ) {
        return position.getLatitude() > 0
                && position.getSpeed() > 0
                && position.getLongitude() > 0
                && position.getDeviceTime().after( super.date );
    }

    protected boolean check (
            final Point point
    ) {
        return this.objectIsNotNull( point )
                && this.objectIsNotNull( point.getLatitude() )
                && this.objectIsNotNull( point.getLongitude() );
    }

    protected boolean check (
            final ReqCar reqCar
    ) {
        return this.objectIsNotNull( reqCar )
                && reqCar.getPatrulPassportSeries() != null
                && reqCar.getPatrulPassportSeries().length() > 1
                && reqCar.getPatrulPassportSeries().compareTo( "null" ) != 0;
    }

    protected boolean calculateDistanceInSquare (
            final List< Point > pointList,
            final Row row
    ) {
        boolean result = false;
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
            j = i;
        }
        return result;
    }

    protected int checkDifference ( final int integer ) {
        return integer > 0 && integer < 100 ? integer : 10;
    }

    protected double calculate (
            final Point point,
            final Row row
    ) {
        return 12742 * asin( sqrt( 0.5 - cos( ( row.getDouble( "latitude" ) - point.getLatitude() ) * P ) / 2
                + cos( point.getLatitude() * P) * cos( row.getDouble( "latitude" ) * P )
                * ( 1 - cos( ( row.getDouble( "longitude" ) - point.getLongitude() ) * P ) ) / 2 ) ) * 1000;
    }

    protected boolean checkCarNumber (
            final String carNumber
    ) {
        return CassandraDataControlForEscort
                .getInstance()
                .getRowFromEscortKeyspace(
                        CassandraTables.TUPLE_OF_CAR,
                        "gosnumber",
                        carNumber
                ) == null
                && CassandraDataControl
                .getInstance()
                .getRowFromTabletsKeyspace(
                        CassandraTables.CARS,
                        "gosnumber",
                        carNumber
                ) == null;
    }

    protected boolean checkParams (
            final Patrul patrul,
            final Map< String, Long > params
    ) {
        return switch ( params.size() ) {
            case 1 -> params.containsKey( "viloyat" ) && Objects.equals(
                    patrul.getPatrulRegionData().getRegionId(), params.get( "viloyat" ) );
            case 2 -> ( params.containsKey( "viloyat" ) && Objects.equals(
                    patrul.getPatrulRegionData().getRegionId(), params.get( "viloyat" ) ) )
                    && ( params.containsKey( "tuman" ) && Objects.equals(
                    patrul.getPatrulRegionData().getDistrictId(), params.get( "tuman" ) ) );
            default -> ( params.containsKey( "tuman" ) && Objects.equals(
                    patrul.getPatrulRegionData().getDistrictId(), params.get( "tuman" ) ) )
                    && ( params.containsKey( "viloyat" ) && Objects.equals(
                    patrul.getPatrulRegionData().getRegionId(), params.get( "viloyat" ) ) )
                    && ( params.containsKey( "mahalla" ) && Objects.equals(
                    patrul.getPatrulRegionData().getMahallaId(), params.get( "mahalla" ) ) );
        };
    }
}
