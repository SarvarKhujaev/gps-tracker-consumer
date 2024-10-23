package com.ssd.mvd.inspectors;

import com.ssd.mvd.kafka.kafkaConfigs.KafkaProducerInterceptor;
import com.ssd.mvd.annotations.EntityConstructorAnnotation;
import com.ssd.mvd.entity.ApiResponseModel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.springframework.scheduling.annotation.Async;
import reactor.core.publisher.Mono;

@com.ssd.mvd.annotations.ImmutableEntityAnnotation
@com.ssd.mvd.annotations.ServiceParametrAnnotation( propertyGroupName = "LOGGER_WITH_JSON_LAYOUT" )
public class LogInspector extends WebFluxInspector {
    protected LogInspector () {
        super( LogInspector.class );
    }

    @EntityConstructorAnnotation(
            permission = {
                    AnnotationInspector.class,
                    KafkaProducerInterceptor.class
            }
    )
    protected <T extends UuidInspector> LogInspector ( @lombok.NonNull final Class<T> instance ) {
        super( LogInspector.class );

        AnnotationInspector.checkCallerPermission( instance, LogInspector.class );
        AnnotationInspector.checkAnnotationIsImmutable( LogInspector.class );
    }

    private final static Logger LOGGER = LogManager.getLogger( "LOGGER_WITH_JSON_LAYOUT" );

    @Async( value = "LogInspector" )
    protected void logging ( @lombok.NonNull @com.typesafe.config.Optional final Object o ) {
        LOGGER.info(
                String.join(
                        SPACE_WITH_DOUBLE_DOTS,
                        o.getClass().getName(),
                        "was closed successfully at",
                        newDate().toString()
                )
        );
    }

    @Async( value = "LogInspector" )
    protected void logging ( @lombok.NonNull @com.typesafe.config.Optional final String message ) {
        LOGGER.info( message );
    }

    protected Mono< ApiResponseModel > logging ( @lombok.NonNull @com.typesafe.config.Optional final Throwable error ) {
        LOGGER.error(
                String.join(
                        SPACE_WITH_DOUBLE_DOTS,
                        "Error",
                        error.getMessage()
                )
        );

        return super.getErrorResponse();
    }

    @Async( value = "LogInspector" )
    protected void logging (
            @lombok.NonNull @com.typesafe.config.Optional final Throwable error,
            @lombok.NonNull @com.typesafe.config.Optional final Object o
    ) {
        LOGGER.error("Error: {} and reason: {}: ", error.getMessage(), o );
    }

    @Async( value = "LogInspector" )
    protected void logging ( @lombok.NonNull @com.typesafe.config.Optional final Class<? extends CollectionsInspector> clazz ) {
        LOGGER.info(
                String.join(
                        SPACE_WITH_DOUBLE_DOTS,
                        clazz.getName(),
                        "was created at",
                        newDate().toString()
                )
        );
    }
}
