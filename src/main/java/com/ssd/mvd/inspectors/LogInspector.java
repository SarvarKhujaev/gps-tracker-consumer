package com.ssd.mvd.inspectors;

import com.ssd.mvd.entity.ApiResponseModel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Mono;

public class LogInspector extends DataValidateInspector {
    private final Logger LOGGER = LogManager.getLogger();

    public Logger getLOGGER() { return LOGGER; }

    public Mono< ApiResponseModel > logging ( Throwable error ) {
        this.getLOGGER().error("Error: {}", error.getMessage() );
        return super.getErrorResponse().get(); }

    public void logging ( Throwable error, Object o ) {
        this.getLOGGER().error("Error: {} and reason: {}: ", error.getMessage(), o ); }

    public void logging ( String message ) { this.getLOGGER().info( message ); }
}
