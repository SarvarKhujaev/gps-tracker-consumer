package com.ssd.mvd.inspectors;

import java.util.Calendar;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;

public class TimeInspector extends Inspector {
    protected TimeInspector () {}

    protected final Date date = new Date( 1605006666774L );

    protected Date newDate () {
        return new Date();
    }

    protected Calendar calendarInstance () {
        return Calendar.getInstance();
    }

    protected long getTimeDifference (
            final long timestamp,
            final Instant instant
    ) {
        return Math.abs( timestamp + Duration.between( Instant.now(), instant ).getSeconds() );
    }
}
