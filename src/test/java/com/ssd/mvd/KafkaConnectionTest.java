package com.ssd.mvd;

import com.ssd.mvd.database.CassandraDataControl;
import com.ssd.mvd.inspectors.EntitiesInstances;
import com.ssd.mvd.kafka.KafkaDataControl;
import com.ssd.mvd.entity.Position;

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
                .sendMessageToKafka(
                        EntitiesInstances.REQ_CAR.get().generate(
                                CassandraDataControl
                                        .getInstance()
                                        .getRowFromTabletsKeyspace(
                                                EntitiesInstances.REQ_CAR.get(),
                                                this.uuid.toString()
                                        ).get()
                        )
                );

        KafkaDataControl
                .getInstance()
                .sendMessageToKafka(
                        EntitiesInstances.TUPLE_OF_CAR.get().generate(
                                CassandraDataControl
                                        .getInstance()
                                        .getRowFromTabletsKeyspace(
                                                EntitiesInstances.TUPLE_OF_CAR.get(),
                                                this.uuid.toString(),
                                                "uuid"
                                        ).get()
                        )
                );

        KafkaDataControl
                .getInstance()
                .sendMessageToKafka( new Position() );
    }
}
