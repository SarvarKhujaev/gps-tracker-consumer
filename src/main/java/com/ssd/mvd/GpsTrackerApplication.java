package com.ssd.mvd;

import com.ssd.mvd.entity.Position;
import com.ssd.mvd.inspectors.avro.AvroSchemaInspector;

import org.springframework.context.ApplicationContext;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.UUID;

@SpringBootApplication
public class GpsTrackerApplication {
    public static ApplicationContext context;

    public static void main( final String[] args ) {
//        context = SpringApplication.run( GpsTrackerApplication.class, args );
//        CassandraDataControl.getInstance().setCassandraTablesAndTypesRegister();
//        KafkaDataControl.getKafkaDataControl().start();

        final Position position = new Position();

        position.setIcon( "test" );
        position.setIcon2( "test" );
        position.setCarType( "test" );
        position.setPatrulName( "test" );
        position.setPoliceType( "test" );
        position.setCarGosNumber( "test" );

        position.setLatitude( 65.6 );
        position.setLongitude( 65.6 );
        position.setLatitudeOfTask( 65.6 );
        position.setLongitudeOfTask( 65.6 );

        position.setRegionId( 65L );
        position.setMahallaId( 65L );
        position.setDistrictId( 65L );

        position.setPatrulUUID( UUID.randomUUID() );

        System.out.println(
                AvroSchemaInspector.deserialize(
                        AvroSchemaInspector.generateSchema( position ).toString().getBytes(),
                        position
                )
        );
    }
}
