package com.ssd.mvd.inspectors;

import java.util.Calendar;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;

public class TimeInspector extends CollectionsInspector {
    protected TimeInspector () {}

    public final static int DAY_IN_SECOND = 86400;
    public final static long FIVE_HOURS = 5L * 60 * 60 * 1000;

    protected final static Date date = new Date( 1605006666774L );

    protected final synchronized Date newDate () {
        return new Date();
    }

    protected final synchronized Date newDate (
            final long timeInterval
    ) {
        return new Date( timeInterval );
    }

    protected final synchronized Calendar calendarInstance () {
        return Calendar.getInstance();
    }

    protected final synchronized long getTimeDifference (
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
