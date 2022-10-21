package com.ssd.mvd;

import com.ssd.mvd.kafka.KafkaDataControl;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class GpsTrackerApplication {
    public static ApplicationContext context;

    public static void main( String[] args ) {
        context = SpringApplication.run( GpsTrackerApplication.class, args );
        KafkaDataControl.getInstance().start(); }
}
