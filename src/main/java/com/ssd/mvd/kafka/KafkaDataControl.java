package com.ssd.mvd.kafka;

import java.util.*;
import java.util.function.Supplier;

import reactor.core.scheduler.Schedulers;

import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Consumed;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.clients.producer.ProducerConfig;

import com.ssd.mvd.entity.Position;
import com.ssd.mvd.publisher.CustomPublisher;
import com.ssd.mvd.inspectors.EntitiesInstances;
import com.ssd.mvd.subscribers.CustomSubscriber;
import com.ssd.mvd.database.CassandraDataControl;

import com.ssd.mvd.interfaces.ServiceCommonMethods;
import com.ssd.mvd.interfaces.KafkaEntitiesCommonMethods;

import com.ssd.mvd.kafka.kafkaConfigs.KafkaTopics;
import com.ssd.mvd.kafka.kafkaConfigs.KafkaOptionsAndParams;
import com.ssd.mvd.kafka.kafkaConfigs.KafkaProducerInterceptor;

public final class KafkaDataControl extends KafkaOptionsAndParams implements ServiceCommonMethods {
    private KafkaStreams kafkaStreams;
    private final Properties properties = new Properties();
    private final StreamsBuilder builder = new StreamsBuilder();
    private static KafkaDataControl INSTANCE = new KafkaDataControl();

    private final Supplier< WeakHashMap< String, Object > > getKafkaSenderOptions = () -> {
        final WeakHashMap< String, Object > options = super.newMap();

        options.put( ProducerConfig.ACKS_CONFIG, KAFKA_ACKS_CONFIG );

        // The number of times to retry sending a message if it fails.
        options.put( ProducerConfig.RETRIES_CONFIG, RETRIES_CONFIG );

        // The maximum time to wait before sending a batch to the broker
        options.put( ProducerConfig.LINGER_MS_CONFIG, LINGER_MS_CONFIG );

        // The maximum size of the batch to send to the broker
        options.put( ProducerConfig.BATCH_SIZE_CONFIG, BATCH_SIZE_CONFIG );

        options.put( ProducerConfig.CLIENT_ID_CONFIG, GROUP_ID_FOR_KAFKA );

        // The maximum amount of memory to use for buffering messages
        options.put( ProducerConfig.BUFFER_MEMORY_CONFIG, BUFFER_MEMORY_CONFIG );

        // The maximum time to wait for a response from the broker
        options.put( ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_MS_CONFIG );

        // The maximum number of outstanding requests to send to the broker
        options.put( ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION );

        // The compression algorithm to use for messages
        options.put(
                ProducerConfig.COMPRESSION_TYPE_CONFIG,
                checkContextOrReturnDefaultValue(
                        "variables.KAFKA_VARIABLES.COMPRESSION_TYPE_CONFIG",
                        CompressionType.LZ4.name
                )
        );

        options.put( ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER );

        // The maximum age of metadata in milliseconds
        options.put( ProducerConfig.METADATA_MAX_AGE_CONFIG, METADATA_MAX_AGE_CONFIG );

        options.put( ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, KafkaProducerInterceptor.class.getName() );

        options.put( ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, EntitiesInstances.KAFKA_STRING_SERIALIZER.get().getClass().getName() );
        options.put( ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, EntitiesInstances.KAFKA_BYTE_SERIALIZER.get().getClass().getName() );

        return options;
    };

    private final KafkaSender< String, byte[] > kafkaSender = KafkaSender.create(
            SenderOptions.< String, byte[] >create( this.getKafkaSenderOptions.get() )
                    .scheduler( Schedulers.parallel() )
                    .maxInFlight( KAFKA_SENDER_MAX_IN_FLIGHT )
                    .withKeySerializer( EntitiesInstances.KAFKA_STRING_SERIALIZER.get() )
                    .withValueSerializer( EntitiesInstances.KAFKA_BYTE_SERIALIZER.get() )
    );

    public static KafkaDataControl getKafkaDataControl () {
        return INSTANCE != null ? INSTANCE : ( INSTANCE = new KafkaDataControl() );
    }

    private KafkaDataControl () {
        super( KafkaDataControl.class );
        super.logging( this.getClass() );
    }

    private final Supplier< Properties > setStreamProperties = () -> {
        this.properties.clear();

        // The number of times to retry sending a message if it fails.
        this.properties.put( StreamsConfig.CLIENT_ID_CONFIG, GROUP_ID_FOR_KAFKA );

        // The maximum time to wait for a response from the broker
        this.properties.put( StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_MS_CONFIG );
        this.properties.put( StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER );

        // The maximum age of metadata in milliseconds
        this.properties.put( StreamsConfig.METADATA_MAX_AGE_CONFIG, METADATA_MAX_AGE_CONFIG );

        this.properties.put( StreamsConfig.APPLICATION_ID_CONFIG, GROUP_ID_FOR_KAFKA );
        this.properties.put( StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, EntitiesInstances.KAFKA_STRING_SERIALIZER.getClass().getName() );
        this.properties.put( StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, EntitiesInstances.KAFKA_BYTE_SERIALIZER.getClass().getName() );

        return this.properties;
    };

    public void start () {
        final KStream< String, byte[] > kStream = this.builder.stream(
                KafkaTopics.RAW_GPS_LOCATION_TOPIC.getTopicName(),
                Consumed.with( EntitiesInstances.STRING_SERDE.get(), Serdes.ByteArray() )
        );

        kStream.mapValues(
                values -> CassandraDataControl
                        .getInstance()
                        .saveCarLocation
                        .apply( new Position( EntitiesInstances.class ) )
        );

        this.kafkaStreams = new KafkaStreams( this.builder.build(), this.setStreamProperties.get() );
        this.kafkaStreams.start();
    }

    public void sendMessageToKafka (
            final KafkaEntitiesCommonMethods kafkaEntitiesCommonMethods
    ) {
        this.kafkaSender
                .createOutbound()
                .send( CustomPublisher.from( kafkaEntitiesCommonMethods ) )
                .then()
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
        this.properties.clear();
        super.logging( this );
        this.kafkaSender.close();
        this.kafkaStreams.close();
    }
}
