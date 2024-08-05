package com.ssd.mvd.inspectors;

import com.ssd.mvd.interfaces.ServiceCommonMethods;
import com.ssd.mvd.entity.ApiResponseModel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import reactor.core.publisher.Mono;

public class LogInspector extends WebFluxInspector {
    protected LogInspector() {}

    private static final Logger LOGGER = LogManager.getLogger();

    protected final synchronized Mono< ApiResponseModel > logging (
            final Throwable error
    ) {
        LOGGER.error("Error: {}", error.getMessage() );
        return super.getErrorResponse();
    }

    protected final synchronized void logging (
            final Throwable error,
            final Object o
    ) {
        LOGGER.error("Error: {} and reason: {}: ", error.getMessage(), o );
    }

    protected final synchronized void logging (
            final String message
    ) {
        LOGGER.info( message );
    }

    protected final synchronized void logging ( final Object o ) {
        LOGGER.info( o.getClass().getName() + " was closed successfully at: " + super.newDate() );
    }

    protected final synchronized void logging ( final Class<? extends ServiceCommonMethods> clazz ) {
        LOGGER.info(
                String.join(
                        "",
                        clazz.getName(),
                        " was created at: ",
                        super.newDate().toString()
                )
        );
    }
}
