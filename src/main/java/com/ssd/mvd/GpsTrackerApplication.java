package com.ssd.mvd;

import com.ssd.mvd.database.CassandraDataControl;
import com.ssd.mvd.kafka.KafkaDataControl;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
public class GpsTrackerApplication {
    public static ApplicationContext context;

    public static void main( final String[] args ) {
        context = SpringApplication.run( GpsTrackerApplication.class, args );
        CassandraDataControl.getInstance().setCassandraTablesAndTypesRegister();
        KafkaDataControl.getInstance().start();
    }
}
