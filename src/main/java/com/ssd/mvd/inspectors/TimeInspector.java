package com.ssd.mvd.inspectors;

import com.ssd.mvd.annotations.EntityConstructorAnnotation;

import java.util.concurrent.atomic.AtomicReference;
import java.util.Calendar;
import java.util.Date;

import java.time.Duration;
import java.time.Instant;

@com.ssd.mvd.annotations.ImmutableEntityAnnotation
public class TimeInspector extends CollectionsInspector {
    protected TimeInspector () {
        super( TimeInspector.class );
    }

    @EntityConstructorAnnotation( permission = DataValidateInspector.class )
    protected <T extends UuidInspector> TimeInspector ( @lombok.NonNull final Class<T> instance ) {
        super( TimeInspector.class );

        AnnotationInspector.checkCallerPermission( instance, TimeInspector.class );
        AnnotationInspector.checkAnnotationIsImmutable( TimeInspector.class );
    }

    protected final static AtomicReference< Calendar > end = EntitiesInstances.generateAtomicEntity( Calendar.getInstance() );
    protected final static AtomicReference< Calendar > start = EntitiesInstances.generateAtomicEntity( Calendar.getInstance() );

    public final static int DAY_IN_SECOND = 86400;
    public final static long FIVE_HOURS = 5L * 60 * 60 * 1000;

    protected final static Date date = new Date( 1605006666774L );
    protected final static Duration DURATION = Duration.ofMillis( 100 );

    public static synchronized Date newDate () {
        return new Date();
    }

    public static synchronized Date newDate (
            final long timeInterval
    ) {
        return new Date( timeInterval );
    }

    protected final synchronized Calendar calendarInstance () {
        return Calendar.getInstance();
    }

    public static synchronized long getTimeDifference (
            final long timestamp,
            final Instant instant
    ) {
        return Math.abs( timestamp + Duration.between( Instant.now(), instant ).getSeconds() );
    }

    protected final synchronized int getTimeDifference(
            final Date startDate,
            final Date endDate
    ) {
        return (int) Math.abs(
                Duration.between(
                        startDate.toInstant(),
                        endDate.toInstant()
                ).toDays()
        );
    }
}
