package com.ssd.mvd.inspectors;

import static java.lang.Math.*;
import com.datastax.driver.core.Row;

import java.util.Map;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

import com.ssd.mvd.entity.Point;
import com.ssd.mvd.entity.ReqCar;
import com.ssd.mvd.entity.Request;
import com.ssd.mvd.entity.Position;
import com.ssd.mvd.GpsTrackerApplication;
import com.ssd.mvd.entity.patrulDataSet.Patrul;
import com.ssd.mvd.database.CassandraDataControl;
import com.ssd.mvd.database.CassandraDataControlForEscort;

public class DataValidateInspector extends TimeInspector {
    protected DataValidateInspector () {}

    private static final double P = PI / 180;
    private final static DataValidateInspector DATA_VALIDATE_INSPECTOR = new DataValidateInspector();

    public static DataValidateInspector getInstance () {
        return DATA_VALIDATE_INSPECTOR;
    }

    protected final synchronized <T> boolean objectIsNotNull ( final T value ) {
        return Objects.nonNull( value );
    }

    protected final synchronized boolean check (
            final String trackerId
    ) {
        return Inspector.tupleOfCarMap.containsKey( trackerId )
                && Inspector.trackerInfoMap.containsKey( trackerId );
    }

    protected final synchronized boolean check (
            final Row row
    ) {
        return row.getDouble( "longitude" ) > 0 && row.getDouble( "latitude" ) > 0;
    }

    protected final synchronized boolean check (
            final Request request
    ) {
        return request.getStartTime() == null && request.getEndTime() == null;
    }

    protected final synchronized boolean check (
            final Position position
    ) {
        return position.getLatitude() > 0
                && position.getSpeed() > 0
                && position.getLongitude() > 0
                && position.getDeviceTime().after( date );
    }

    protected final synchronized boolean check (
            final ReqCar reqCar
    ) {
        return this.objectIsNotNull( reqCar )
                && reqCar.getPatrulPassportSeries() != null
                && reqCar.getPatrulPassportSeries().length() > 1
                && reqCar.getPatrulPassportSeries().compareTo( "null" ) != 0;
    }

    protected final synchronized boolean calculateDistanceInSquare (
            final List< Point > pointList,
            final Row row
    ) {
        boolean result = false;
        int j = pointList.size() - 1;
        for ( int i = 0; i < pointList.size(); i++ ) {
            if (
                    ( pointList.get( i ).getLatitude() < row.getDouble( "latitude" )
                        && pointList.get( j ).getLatitude() >= row.getDouble( "latitude" )
                        || pointList.get( j ).getLatitude() < row.getDouble( "latitude" )
                        && pointList.get( i ).getLatitude() >= row.getDouble( "latitude" ) )
                        && ( pointList.get( i ).getLongitude() + ( row.getDouble( "latitude" )
                        - pointList.get( i ).getLatitude() ) / ( pointList.get( j ).getLatitude() - pointList.get( j ).getLongitude() )
                        * ( pointList.get( j ).getLatitude() - pointList.get( i ).getLatitude() ) < row.getDouble( "longitude" ) )
            ) {
                result = !result;
            }

            j = i;
        }

        return result;
    }

    protected final synchronized int checkDifference ( final int integer ) {
        return integer > 0 && integer < 100 ? integer : 10;
    }

    protected final synchronized double calculate (
            final Point point,
            final Row row
    ) {
        return 12742 * asin( sqrt( 0.5 - cos( ( row.getDouble( "latitude" ) - point.getLatitude() ) * P ) / 2
                + cos( point.getLatitude() * P) * cos( row.getDouble( "latitude" ) * P )
                * ( 1 - cos( ( row.getDouble( "longitude" ) - point.getLongitude() ) * P ) ) / 2 ) ) * 1000;
    }

    protected final synchronized boolean checkCarNumber (
            final String carNumber
    ) {
        return CassandraDataControlForEscort
                .getInstance()
                .getRowFromTabletsKeyspace(
                        EntitiesInstances.TUPLE_OF_CAR,
                        "gosnumber",
                        carNumber
                ) == null
                && CassandraDataControl
                    .getInstance()
                    .getRowFromTabletsKeyspace(
                            EntitiesInstances.REQ_CAR,
                            "gosnumber",
                            carNumber
                    ) == null;
    }

    protected final synchronized boolean checkParams (
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

    /*
    принимает запись из БД
    проверяет что запись не пуста
    и заполняет параметры объекта по заданной логике
    */
    protected final synchronized <T> void checkAndSetParams (
            final T object,
            final Consumer< T > customConsumer
    ) {
        Optional.ofNullable( object ).ifPresent( customConsumer );
    }

    /*
    получает в параметрах название параметра из файла application.yaml
    проверят что context внутри main класса GpsTabletsServiceApplication  инициализирован
    и среди параметров сервиса сузествует переданный параметр
    */
    protected final synchronized int checkContextOrReturnDefaultValue (
            final String paramName,
            final int defaultValue
    ) {
        return this.objectIsNotNull( GpsTrackerApplication.context )
                && this.objectIsNotNull(
                        GpsTrackerApplication
                                .context
                                .getEnvironment()
                                .getProperty( paramName )
                )
                ? Integer.parseInt(
                        GpsTrackerApplication
                                .context
                                .getEnvironment()
                                .getProperty( paramName )
                )
                : defaultValue;
    }

    public final synchronized String checkContextOrReturnDefaultValue (
            final String paramName,
            final String defaultValue
    ) {
        return this.objectIsNotNull( GpsTrackerApplication.context )
                && this.objectIsNotNull(
                        GpsTrackerApplication
                                .context
                                .getEnvironment()
                                .getProperty( paramName )
                )
                ? GpsTrackerApplication
                        .context
                        .getEnvironment()
                        .getProperty( paramName )
                : defaultValue;
    }
}
