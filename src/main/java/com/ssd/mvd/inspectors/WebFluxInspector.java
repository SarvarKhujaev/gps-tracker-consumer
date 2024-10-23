package com.ssd.mvd.inspectors;

import com.ssd.mvd.annotations.EntityConstructorAnnotation;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import reactor.core.publisher.ParallelFlux;
import reactor.core.scheduler.Schedulers;
import reactor.core.publisher.Flux;

import java.util.function.Predicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.Collection;

@SuppressWarnings(
        value = "хранит все функции для более компактного и удобного хранения всех основных функции WebFlux"
)
@com.ssd.mvd.annotations.ImmutableEntityAnnotation
@com.ssd.mvd.annotations.ServiceParametrAnnotation( propertyGroupName = "WEB_FLUX_PARAMS" )
public class WebFluxInspector extends Inspector {
    protected static final int RESULT_COUNT = checkContextOrReturnDefaultValue(
            "variables.WEB_FLUX_PARAMS.RESULT_COUNT",
            1000
    );

    protected final synchronized ParallelFlux< Row > convertValuesToParallelFlux (
            @lombok.NonNull final ResultSet resultSet,
            final int parallelsCount
    ) {
        return Flux.fromStream( resultSet.all().stream() )
                .parallel( super.checkDifference( parallelsCount ) )
                .runOn( Schedulers.parallel() );
    }

    @EntityConstructorAnnotation( permission = LogInspector.class )
    protected <T extends UuidInspector> WebFluxInspector ( @lombok.NonNull final Class<T> instance ) {
        super( WebFluxInspector.class );

        AnnotationInspector.checkCallerPermission( instance, WebFluxInspector.class );
        AnnotationInspector.checkAnnotationIsImmutable( WebFluxInspector.class );
    }

    @lombok.NonNull
    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_ -> !null" )
    protected final synchronized <T> ParallelFlux< T > convertValuesToParallelFlux (
            @lombok.NonNull final Collection<T> collection
    ) {
        return Flux.fromStream( collection.stream() )
                .parallel( super.checkDifference( collection.size() ) )
                .runOn( Schedulers.parallel() );
    }

    @lombok.NonNull
    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_, _ -> !null" )
    protected final synchronized <T, U> Flux< U > convertValuesToParallelFlux (
            @lombok.NonNull final Collection<T> collection,
            @lombok.NonNull final Function< T, U > customFunction
    ) {
        return Flux.fromStream( collection.stream() )
                .parallel( super.checkDifference( collection.size() ) )
                .map( customFunction )
                .runOn( Schedulers.parallel() )
                .sequential()
                .publishOn( Schedulers.single() );
    }

    @lombok.NonNull
    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_, _, _ -> !null" )
    protected final synchronized <T, U> Flux< U > convertValuesToParallelFlux (
            @lombok.NonNull final Collection<T> collection,
            @lombok.NonNull final Predicate< T > customPredicate,
            @lombok.NonNull final Function< T, U > customFunction
    ) {
        return Flux.fromStream( collection.stream() )
                .parallel( super.checkDifference( collection.size() ) )
                .filter( customPredicate )
                .map( customFunction )
                .runOn( Schedulers.parallel() )
                .sequential()
                .publishOn( Schedulers.single() );
    }

    @lombok.NonNull
    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_ -> !null" )
    protected final synchronized ParallelFlux< Row > convertValuesToParallelFlux (
            @lombok.NonNull final ResultSet resultSet
    ) {
        return Flux.fromStream( resultSet.all().stream() )
                .parallel( super.checkDifference( resultSet.all().size() ) )
                .runOn( Schedulers.parallel() );
    }

    @lombok.NonNull
    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_, _ -> !null" )
    protected final synchronized <T> Flux< T > convertValuesToParallelFlux (
            @lombok.NonNull final ResultSet resultSet,
            @lombok.NonNull final Function< Row, T > customFunction
    ) {
        return Flux.fromStream( resultSet.all().stream() )
                .parallel( super.checkDifference( resultSet.all().size() ) )
                .map( customFunction )
                .runOn( Schedulers.parallel() )
                .sequential()
                .publishOn( Schedulers.single() );
    }

    @lombok.NonNull
    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_, _, _ -> !null" )
    protected final synchronized <T> Flux< T > convertValuesToParallelFlux (
            @lombok.NonNull final ResultSet resultSet,
            @lombok.NonNull final Predicate< Row > customPredicate,
            @lombok.NonNull final Function< Row, T > customFunction
    ) {
        return Flux.fromStream( resultSet.all().stream() )
                .parallel( super.checkDifference( resultSet.all().size() ) )
                .filter( customPredicate )
                .map( customFunction )
                .runOn( Schedulers.parallel() )
                .sequential()
                .publishOn( Schedulers.single() );
    }

    @lombok.NonNull
    @lombok.Synchronized
    @org.jetbrains.annotations.Contract( value = "_ -> !null" )
    protected static synchronized Flux< Integer > convertValuesToParallelFlux (
            @lombok.NonNull final Consumer< Integer > customConsumer
    ) {
        return Flux.range( 0, RESULT_COUNT )
                .parallel( (int) (WebFluxInspector.RESULT_COUNT / TimeInspector.DURATION.toMillis() ) )
                .runOn( Schedulers.parallel() )
                .map( integer -> {
                    customConsumer.accept( integer );
                    return integer;
                } )
                .sequential()
                .publishOn( Schedulers.parallel() );
    }
}
