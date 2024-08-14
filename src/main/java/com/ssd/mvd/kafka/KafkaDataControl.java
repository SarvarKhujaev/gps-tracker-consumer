package com.ssd.mvd.kafka;

import java.util.*;
import java.util.function.Supplier;

import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import com.ssd.mvd.entity.Position;
import com.ssd.mvd.inspectors.SerDes;
import com.ssd.mvd.publisher.CustomPublisher;
import com.ssd.mvd.subscribers.CustomSubscriber;
import com.ssd.mvd.database.CassandraDataControl;
import com.ssd.mvd.kafka.kafkaConfigs.KafkaTopics;
import com.ssd.mvd.interfaces.ServiceCommonMethods;
import com.ssd.mvd.interfaces.KafkaEntitiesCommonMethods;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Consumed;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.clients.producer.ProducerConfig;

public final class KafkaDataControl extends SerDes implements ServiceCommonMethods {
    private final static Serde<String> stringserdes = Serdes.String();

    private final String KAFKA_BROKER = super.checkContextOrReturnDefaultValue(
            "variables.KAFKA_VARIABLES.KAFKA_BROKER",
            "localhost:9092"
    );

    private final String GROUP_ID_FOR_KAFKA = super.checkContextOrReturnDefaultValue(
            "variables.KAFKA_VARIABLES.GROUP_ID_FOR_KAFKA",
            this.getClass().getName()
    );

    private KafkaStreams kafkaStreams;
    private final Properties properties = new Properties();
    private final StreamsBuilder builder = new StreamsBuilder();
    private static KafkaDataControl INSTANCE = new KafkaDataControl();

    private final Supplier< Map< String, Object > > getKafkaSenderOptions = () -> Map.of(
            ProducerConfig.ACKS_CONFIG, super.checkContextOrReturnDefaultValue(
                    "variables.KAFKA_VARIABLES.KAFKA_ACKS_CONFIG",
                    "-1"
            ),
            ProducerConfig.MAX_BLOCK_MS_CONFIG, super.checkContextOrReturnDefaultValue(
                    "variables.KAFKA_VARIABLES.KAFKA_MAX_BLOCK_MS_CONFIG",
                    33554432 * 20
            ),
            ProducerConfig.CLIENT_ID_CONFIG, this.GROUP_ID_FOR_KAFKA,
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.KAFKA_BROKER,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class
    );

    private final KafkaSender< String, String > kafkaSender = KafkaSender.create(
            SenderOptions.< String, String >create( this.getKafkaSenderOptions.get() )
                    .maxInFlight(
                            super.checkContextOrReturnDefaultValue(
                                    "variables.KAFKA_VARIABLES.KAFKA_SENDER_MAX_IN_FLIGHT",
                                    1024
                            )
                    )
    );

    public static KafkaDataControl getInstance () {
        return INSTANCE != null ? INSTANCE : ( INSTANCE = new KafkaDataControl() );
    }

    private KafkaDataControl () {
        super.logging( this.getClass() );
    }

    private final Supplier< Properties > setStreamProperties = () -> {
            this.properties.clear();
            this.properties.put( StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.KAFKA_BROKER );
            this.properties.put( StreamsConfig.APPLICATION_ID_CONFIG, this.GROUP_ID_FOR_KAFKA );
            this.properties.put( StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringserdes.getClass().getName() );
            this.properties.put( StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, stringserdes.getClass().getName() );
            return this.properties;
    };

    public void start () {
        final KStream< String, String > kStream = this.builder.stream(
                KafkaTopics.RAW_GPS_LOCATION_TOPIC.getTopicName(),
                Consumed.with( Serdes.String(), Serdes.String() )
        );

        kStream.mapValues(
                values -> CassandraDataControl
                        .getInstance()
                        .saveCarLocation
                        .apply( super.deserialize( values, Position.class ) )
        );

        this.kafkaStreams = new KafkaStreams( this.builder.build(), this.setStreamProperties.get() );
        this.kafkaStreams.start();
    }

    public void sendMessageToKafka (
            final KafkaEntitiesCommonMethods kafkaEntitiesCommonMethods
    ) {
        this.kafkaSender
                .createOutbound()
                .send(
                        CustomPublisher.generate(
                                kafkaEntitiesCommonMethods.getTopicName(),
                                super.serialize( kafkaEntitiesCommonMethods )
                        )
                ).then()
                .doOnError( this::close )
                .doOnSuccess( success -> super.logging( kafkaEntitiesCommonMethods.getSuccessMessage() ) )
                .subscribe(
                        new CustomSubscriber<>(
                                topicName -> super.logging( kafkaEntitiesCommonMethods.generateMessage() )
                        )
                );
    }

    @Override
    public void close() {
        INSTANCE = null;
        stringserdes.close();
        super.logging( this );
        this.kafkaSender.close();
        this.kafkaStreams.close();
    }
}
