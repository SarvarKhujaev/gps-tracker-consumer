package com.ssd.mvd.inspectors;

import com.ssd.mvd.entity.ApiResponseModel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Mono;

public class LogInspector extends DataValidateInspector {
    private final Logger LOGGER = LogManager.getLogger();

    private Logger getLOGGER() { return LOGGER; }

    protected Mono< ApiResponseModel > logging ( final Throwable error ) {
        this.getLOGGER().error("Error: {}", error.getMessage() );
        return super.getErrorResponse().get(); }

    protected void logging ( final Throwable error, final Object o ) {
        this.getLOGGER().error("Error: {} and reason: {}: ", error.getMessage(), o ); }

    protected void logging ( final String message ) { this.getLOGGER().info( message ); }
}
