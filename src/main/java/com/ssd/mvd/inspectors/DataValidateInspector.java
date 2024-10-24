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
import com.ssd.mvd.entity.patrulDataSet.Patrul;

import com.ssd.mvd.GpsTrackerApplication;
import com.ssd.mvd.annotations.EntityConstructorAnnotation;

import com.ssd.mvd.database.CassandraDataControl;
import com.ssd.mvd.database.CassandraDataControlForEscort;

@com.ssd.mvd.annotations.ImmutableEntityAnnotation
public class DataValidateInspector extends TimeInspector {
    protected DataValidateInspector () {
        super( DataValidateInspector.class );
    }

    @EntityConstructorAnnotation( permission = Inspector.class )
    protected <T extends UuidInspector> DataValidateInspector ( @lombok.NonNull final Class<T> instance ) {
        super( DataValidateInspector.class );

        AnnotationInspector.checkCallerPermission( instance, DataValidateInspector.class );
        AnnotationInspector.checkAnnotationIsImmutable( DataValidateInspector.class );
    }

    private static final double P = PI / 180;

    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_ -> _" )
    protected final synchronized boolean objectIsNotNull (
            @com.typesafe.config.Optional final Object ... o
    ) {
        return o.length == 0
                ? Objects.nonNull( o[0] )
                : convertArrayToStream( o )
                .filter( Objects::nonNull )
                .count() == o.length;
    }

    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_ -> true" )
    protected final synchronized boolean check (
            @lombok.NonNull final String trackerId
    ) {
        return Inspector.tupleOfCarMap.containsKey( trackerId )
                && Inspector.trackerInfoMap.containsKey( trackerId );
    }

    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_ -> true" )
    protected final synchronized boolean check (
            @lombok.NonNull final Row row
    ) {
        return row.getDouble( "longitude" ) > 0 && row.getDouble( "latitude" ) > 0;
    }

    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_ -> true" )
    protected final synchronized boolean check (
            @lombok.NonNull final Request request
    ) {
        return request.getStartTime() == null && request.getEndTime() == null;
    }

    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_ -> true" )
    public static synchronized boolean check (
            @lombok.NonNull final Position position
    ) {
        return position.getLatitude() > 0
                && position.getSpeed() > 0
                && position.getLongitude() > 0
                && position.getDeviceTime().after( date );
    }

    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_ -> _" )
    protected final synchronized boolean check (
            @lombok.NonNull final ReqCar reqCar
    ) {
        return this.objectIsNotNull( reqCar )
                && reqCar.getPatrulPassportSeries() != null
                && reqCar.getPatrulPassportSeries().length() > 1
                && reqCar.getPatrulPassportSeries().compareTo( "null" ) != 0;
    }

    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_, _ -> true" )
    protected final synchronized boolean calculateDistanceInSquare (
            @lombok.NonNull final List< Point > pointList,
            @lombok.NonNull final Row row
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

    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_ -> _" )
    protected final synchronized int checkDifference ( final int integer ) {
        return integer > 0 && integer < 100 ? integer : 10;
    }

    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_, _ -> _" )
    protected final synchronized double calculate (
            @lombok.NonNull final Point point,
            @lombok.NonNull final Row row
    ) {
        return 12742 * asin( sqrt( 0.5 - cos( ( row.getDouble( "latitude" ) - point.getLatitude() ) * P ) / 2
                + cos( point.getLatitude() * P) * cos( row.getDouble( "latitude" ) * P )
                * ( 1 - cos( ( row.getDouble( "longitude" ) - point.getLongitude() ) * P ) ) / 2 ) ) * 1000;
    }

    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_ -> true" )
    protected final synchronized boolean checkCarNumber (
            @lombok.NonNull final String carNumber
    ) {
        return CassandraDataControlForEscort
                .getInstance()
                .getRowFromTabletsKeyspace(
                        EntitiesInstances.TUPLE_OF_CAR.get(),
                        "gosnumber",
                        carNumber
                ).get() == null
                && CassandraDataControl
                    .getInstance()
                    .getRowFromTabletsKeyspace(
                            EntitiesInstances.REQ_CAR.get(),
                            "gosnumber",
                            carNumber
                    ).get() == null;
    }

    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_, _ -> true" )
    protected final synchronized boolean checkParams (
            @lombok.NonNull final Patrul patrul,
            @lombok.NonNull final Map< String, Long > params
    ) {
        return switch ( params.size() ) {
            case 1 -> params.containsKey( "viloyat" ) && Objects.equals(
                    patrul.getPatrulRegionData().getRegionId(), params.get( "viloyat" )
            );
            case 2 -> params.containsKey( "viloyat" ) && Objects.equals(
                            patrul.getPatrulRegionData().getRegionId(), params.get( "viloyat" )
                    )
                    && params.containsKey( "tuman" ) && Objects.equals(
                            patrul.getPatrulRegionData().getDistrictId(), params.get( "tuman" )
                    );
            default -> params.containsKey( "tuman" ) && Objects.equals(
                            patrul.getPatrulRegionData().getDistrictId(), params.get( "tuman" )
                    )
                    && params.containsKey( "viloyat" ) && Objects.equals(
                            patrul.getPatrulRegionData().getRegionId(), params.get( "viloyat" )
                    )
                    && params.containsKey( "mahalla" ) && Objects.equals(
                            patrul.getPatrulRegionData().getMahallaId(), params.get( "mahalla" )
                    );
        };
    }

    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_ -> !null" )
    public static synchronized <T> Optional<T> getOptional (
            @com.typesafe.config.Optional final T object
    ) {
        return Optional.ofNullable( object );
    }

    @SuppressWarnings(
            value = """
                    принимает запись из БД
                    проверяет что запись не пуста
                    и заполняет параметры объекта по заданной логике
                    """
    )
    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_, _ -> _" )
    public static synchronized <T> void checkAndSetParams (
            final T object,
            @lombok.NonNull final Consumer< T > customConsumer
    ) {
        getOptional( object ).ifPresent( customConsumer );
    }

    @SuppressWarnings(
            value = """
                    получает в параметрах название параметра из файла application.yaml
                    проверят что context внутри main класса GpsTabletsServiceApplication  инициализирован
                    и среди параметров сервиса существует переданный параметр
                    """
    )
    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_, _ -> _" )
    protected static synchronized int checkContextOrReturnDefaultValue (
            @lombok.NonNull final String paramName,
            final int defaultValue
    ) {
        return Objects.nonNull( GpsTrackerApplication.context )
                && Objects.nonNull(
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

    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_, _ -> _" )
    public static synchronized String checkContextOrReturnDefaultValue (
            @lombok.NonNull final String paramName,
            @lombok.NonNull final String defaultValue
    ) {
        return Objects.nonNull( GpsTrackerApplication.context )
                && Objects.nonNull(
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
