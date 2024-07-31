package com.ssd.mvd.inspectors;

import com.ssd.mvd.entity.ApiResponseModel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Mono;

public class LogInspector extends WebFluxInspector {
    protected LogInspector() {}

    private final Logger LOGGER = LogManager.getLogger();

    private Logger getLOGGER() { return this.LOGGER; }

    protected final synchronized Mono< ApiResponseModel > logging (
            final Throwable error
    ) {
        this.getLOGGER().error("Error: {}", error.getMessage() );
        return super.getErrorResponse();
    }

    protected final synchronized void logging (
            final Throwable error,
            final Object o
    ) {
        this.getLOGGER().error("Error: {} and reason: {}: ", error.getMessage(), o );
    }

    protected final synchronized void logging (
            final String message
    ) {
        this.getLOGGER().info( message );
    }

    protected final synchronized void logging ( final Object o ) {
        this.getLOGGER().info( o.getClass().getName() + " was closed successfully at: " + super.newDate() );
    }

    protected final synchronized void logging ( final Class<?> clazz ) {
        this.getLOGGER().info(
                String.join(
                        "",
                        clazz.getName(),
                        " was created at: ",
                        super.newDate().toString()
                )
        );
    }
}
