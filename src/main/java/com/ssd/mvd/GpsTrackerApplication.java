package com.ssd.mvd;

import com.ssd.mvd.database.CassandraDataControl;

import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class GpsTrackerApplication {
    public static ApplicationContext context;

    public static void main( final String[] args ) {
        context = SpringApplication.run( GpsTrackerApplication.class, args );
        CassandraDataControl.getInstance().setCassandraTablesAndTypesRegister();
    }
}
