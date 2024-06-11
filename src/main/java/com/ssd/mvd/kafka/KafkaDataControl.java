package com.ssd.mvd.kafka;

import java.util.*;
import com.google.gson.Gson;

import java.util.function.Consumer;
import java.util.function.Supplier;

import com.ssd.mvd.constants.Status;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import com.ssd.mvd.entity.ReqCar;
import com.ssd.mvd.entity.Position;
import com.ssd.mvd.entity.TupleOfCar;
import com.ssd.mvd.inspectors.LogInspector;
import com.ssd.mvd.publisher.CustomPublisher;
import com.ssd.mvd.subscribers.CustomSubscriber;
import com.ssd.mvd.database.CassandraDataControl;
import com.ssd.mvd.interfaces.ServiceCommonMethods;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Consumed;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.clients.producer.ProducerConfig;

public final class KafkaDataControl extends LogInspector implements ServiceCommonMethods {
    private final Gson gson = new Gson();
    private final Properties properties = new Properties();
    private static KafkaDataControl instance = new KafkaDataControl();

    private final String KAFKA_BROKER = super.checkContextOrReturnDefaultValue(
            "variables.KAFKA_VARIABLES.KAFKA_BROKER",
            "localhost:9092"
    );

    private final String GROUP_ID_FOR_KAFKA = super.checkContextOrReturnDefaultValue(
            "variables.KAFKA_VARIABLES.GROUP_ID_FOR_KAFKA",
            KafkaDataControl.getInstance().getClass().getName()
    );

    private final String NEW_TUPLE_OF_CAR_TOPIC = super.checkContextOrReturnDefaultValue(
            "variables.KAFKA_VARIABLES.KAFKA_TOPICS.NEW_TUPLE_OF_CAR_TOPIC",
            Status.NOT_AVAILABLE.name()
    );

    private final String TUPLE_OF_CAR_LOCATION_TOPIC = super.checkContextOrReturnDefaultValue(
            "variables.KAFKA_VARIABLES.KAFKA_TOPICS.TUPLE_OF_CAR_LOCATION_TOPIC",
            Status.NOT_AVAILABLE.name()
    );

    private final String NEW_CAR_TOPIC = super.checkContextOrReturnDefaultValue(
            "variables.KAFKA_VARIABLES.KAFKA_TOPICS.NEW_CAR_TOPIC",
            Status.NOT_AVAILABLE.name()
    );

    private final String WEBSOCKET_SERVICE_TOPIC_FOR_ONLINE = super.checkContextOrReturnDefaultValue(
            "variables.KAFKA_VARIABLES.KAFKA_TOPICS.WEBSOCKET_SERVICE_TOPIC_FOR_ONLINE",
            Status.NOT_AVAILABLE.name()
    );

    private final String RAW_GPS_LOCATION_TOPIC = super.checkContextOrReturnDefaultValue(
            "variables.KAFKA_VARIABLES.KAFKA_TOPICS.RAW_GPS_LOCATION_TOPIC",
            Status.NOT_AVAILABLE.name()
    );

    private KafkaStreams kafkaStreams;
    private final StreamsBuilder builder = new StreamsBuilder();

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
                    .maxInFlight( 1024 )
    );

    private KafkaDataControl () {
        super.logging( "KafkaDataControl was created" );
    }

    public static KafkaDataControl getInstance () {
        return instance != null ? instance : ( instance = new KafkaDataControl() );
    }

    private final Supplier< Properties > setStreamProperties = () -> {
            this.properties.clear();
            this.properties.put( StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.KAFKA_BROKER );
            this.properties.put( StreamsConfig.APPLICATION_ID_CONFIG, this.GROUP_ID_FOR_KAFKA );
            this.properties.put( StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName() );
            this.properties.put( StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName() );
            return this.properties;
    };

    public void start () {
            final KStream< String, String > kStream = this.builder.stream(
                    this.RAW_GPS_LOCATION_TOPIC,
                    Consumed.with( Serdes.String(), Serdes.String() )
            );

            kStream.mapValues(
                    values -> CassandraDataControl
                            .getInstance()
                            .saveCarLocation
                            .apply( this.gson.fromJson( values, Position.class ) )
            );

            this.kafkaStreams = new KafkaStreams( this.builder.build(), this.setStreamProperties.get() );
            this.kafkaStreams.start();
    }

    // записывает позицию от машины Эскорта
    public final Consumer< Position > writeToKafkaEscort = position -> this.kafkaSender
            .createOutbound()
            .send( CustomPublisher.generate( this.TUPLE_OF_CAR_LOCATION_TOPIC, this.gson.toJson( position ) ) )
            .then()
            .doOnError( super::logging )
            .doOnSuccess( success -> super.logging(
                        String.join(
                                "",
                                "Kafka got Escort car location: ",
                                position.getDeviceId(),
                                " at: ",
                                position.getDeviceTime().toString()
                        )
                )
            ).subscribe(
                    new CustomSubscriber<>(
                            value -> super.logging( this.TUPLE_OF_CAR_LOCATION_TOPIC )
                    )
            );

    // записывает позицию от машины патрульного
    public final Consumer< Position > writeToKafkaPosition = position -> this.kafkaSender
            .createOutbound()
            .send( CustomPublisher.generate( this.WEBSOCKET_SERVICE_TOPIC_FOR_ONLINE, this.gson.toJson( position ) ) )
            .then()
            .doOnError( super::logging )
            .doOnSuccess( success -> super.logging(
                        String.join(
                                "",
                                "Kafka got patrul car: ",
                                position.getDeviceId(),
                                " at: ",
                                position.getDeviceTime().toString()
                        )
                    )
            ).subscribe(
                    new CustomSubscriber<>(
                            value -> super.logging( this.WEBSOCKET_SERVICE_TOPIC_FOR_ONLINE )
                    )
            );

    // записывает новую машину Эскорта если она была добавлена в базу
    public final Consumer< TupleOfCar > writeToKafkaTupleOfCar = tupleOfCar ->
            this.kafkaSender
                    .createOutbound()
                    .send( CustomPublisher.generate( this.NEW_TUPLE_OF_CAR_TOPIC, this.gson.toJson( tupleOfCar ) ) )
                    .then()
                    .doOnError( super::logging )
                    .doOnSuccess( success -> super.logging( "Kafka got TupleOfCar: " + tupleOfCar.getTrackerId() ) )
                    .subscribe(
                            new CustomSubscriber<>(
                                    value -> super.logging( this.NEW_TUPLE_OF_CAR_TOPIC )
                            )
                    );

    // записывает новую машину патрульного если она была добавлена в базу
    public final Consumer< ReqCar > writeToKafka = reqCar ->
            this.kafkaSender
                    .createOutbound()
                    .send( CustomPublisher.generate( this.NEW_CAR_TOPIC, this.gson.toJson( reqCar ) ) )
                    .then()
                    .doOnError( super::logging )
                    .doOnSuccess( success -> super.logging( "Kafka got ReqCar: " + reqCar.getTrackerId() ) )
                    .subscribe(
                            new CustomSubscriber<>(
                                    value -> super.logging( this.NEW_CAR_TOPIC )
                            )
                    );

    @Override
    public void close() {
        instance = null;
        super.logging( this );
        this.kafkaSender.close();
        this.kafkaStreams.close();
    }
}
