package com.ssd.mvd;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith( value = Suite.class )
@Suite.SuiteClasses ( value = {
        CassandraConnectionTest.class,
        KafkaConnectionTest.class
} )
public final class GpsTrackerApplicationTests {
}
