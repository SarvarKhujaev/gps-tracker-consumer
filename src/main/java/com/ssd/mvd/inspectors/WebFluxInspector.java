package com.ssd.mvd.inspectors;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import reactor.core.publisher.ParallelFlux;
import reactor.core.scheduler.Schedulers;
import reactor.core.publisher.Flux;

/*
хранит все функции для более компактного и удобного хранения всех основных функции WebFlux
*/
public class WebFluxInspector extends Inspector {
    protected WebFluxInspector() {}

    protected final synchronized ParallelFlux< Row > convertValuesToParallelFlux (
            final ResultSet resultSet,
            final int parallelsCount
    ) {
        return Flux.fromStream( resultSet.all().stream() )
                .parallel( super.checkDifference( parallelsCount ) )
                .runOn( Schedulers.parallel() );
    }
}