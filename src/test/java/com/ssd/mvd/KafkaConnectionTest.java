package com.ssd.mvd;

import com.ssd.mvd.database.CassandraDataControl;
import com.ssd.mvd.constants.CassandraTables;
import com.ssd.mvd.kafka.KafkaDataControl;
import com.ssd.mvd.entity.TupleOfCar;
import com.ssd.mvd.entity.Position;
import com.ssd.mvd.entity.ReqCar;

import com.datastax.driver.core.utils.UUIDs;
import junit.framework.TestCase;
import java.util.UUID;

public final class KafkaConnectionTest extends TestCase {
    private final UUID uuid = UUIDs.timeBased();

    @Override
    public void setUp () {
        super.setName( KafkaDataControl.getInstance().getClass().getName() );
    }

    @Override
    public void tearDown () {
        /*
        closing connection to Kafka
        */
        KafkaDataControl.getInstance().close();
    }

    public void testKafkaConnection () {
        assertNotNull( KafkaDataControl.getInstance() );
    }

    public void testSendMessagesToKafka () {
        KafkaDataControl
                .getInstance()
                .writeToKafka
                .accept(
                        new ReqCar(
                                CassandraDataControl
                                        .getInstance()
                                        .getRowFromTabletsKeyspace(
                                                CassandraTables.CARS,
                                                this.uuid.toString(),
                                                "uuid"
                                        )
                        )
                );

        KafkaDataControl
                .getInstance()
                .writeToKafkaTupleOfCar
                .accept(
                        new TupleOfCar(
                                CassandraDataControl
                                        .getInstance()
                                        .getRowFromTabletsKeyspace(
                                                CassandraTables.TUPLE_OF_CAR,
                                                this.uuid.toString(),
                                                "uuid"
                                        )
                        )
                );

        KafkaDataControl
                .getInstance()
                .writeToKafkaPosition
                .accept( new Position() );

        KafkaDataControl
                .getInstance()
                .writeToKafkaEscort
                .accept( new Position() );
    }
}
