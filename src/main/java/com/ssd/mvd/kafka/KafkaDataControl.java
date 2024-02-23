package com.ssd.mvd.kafka;

import java.util.*;
import com.google.gson.Gson;

import java.util.function.Consumer;
import java.util.function.Supplier;

import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import com.ssd.mvd.entity.ReqCar;
import com.ssd.mvd.entity.Position;
import com.ssd.mvd.entity.TupleOfCar;
import com.ssd.mvd.GpsTrackerApplication;
import com.ssd.mvd.inspectors.LogInspector;
import com.ssd.mvd.publisher.CustomPublisher;
import com.ssd.mvd.subscribers.CustomSubscriber;
import com.ssd.mvd.database.CassandraDataControl;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Consumed;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.clients.producer.ProducerConfig;

@lombok.Data
public final class KafkaDataControl extends LogInspector {
    private final Gson gson = new Gson();
    private Properties properties = new Properties();
    private static KafkaDataControl instance = new KafkaDataControl();

    private final String KAFKA_BROKER = GpsTrackerApplication
            .context
            .getEnvironment()
            .getProperty( "variables.KAFKA_VARIABLES.KAFKA_BROKER" );

    private final String GROUP_ID_FOR_KAFKA = GpsTrackerApplication
            .context
            .getEnvironment()
            .getProperty( "variables.KAFKA_VARIABLES.GROUP_ID_FOR_KAFKA" );

    private final String NEW_TUPLE_OF_CAR_TOPIC = GpsTrackerApplication
            .context
            .getEnvironment()
            .getProperty( "variables.KAFKA_VARIABLES.KAFKA_TOPICS.NEW_TUPLE_OF_CAR_TOPIC" );

    private final String TUPLE_OF_CAR_LOCATION_TOPIC = GpsTrackerApplication
            .context
            .getEnvironment()
            .getProperty( "variables.KAFKA_VARIABLES.KAFKA_TOPICS.TUPLE_OF_CAR_LOCATION_TOPIC" );

    private final String NEW_CAR_TOPIC = GpsTrackerApplication
            .context
            .getEnvironment()
            .getProperty( "variables.KAFKA_VARIABLES.KAFKA_TOPICS.NEW_CAR_TOPIC" );

    private final String WEBSOCKET_SERVICE_TOPIC_FOR_ONLINE = GpsTrackerApplication
            .context
            .getEnvironment()
            .getProperty( "variables.KAFKA_VARIABLES.KAFKA_TOPICS.WEBSOCKET_SERVICE_TOPIC_FOR_ONLINE" );

    private final String RAW_GPS_LOCATION_TOPIC = GpsTrackerApplication
            .context
            .getEnvironment()
            .getProperty( "variables.KAFKA_VARIABLES.KAFKA_TOPICS.RAW_GPS_LOCATION_TOPIC" );

    private KafkaStreams kafkaStreams;
    private final StreamsBuilder builder = new StreamsBuilder();

    private final Supplier< Map< String, Object > > getKafkaSenderOptions = () -> Map.of(
            ProducerConfig.ACKS_CONFIG, "-1",
            ProducerConfig.MAX_BLOCK_MS_CONFIG, 33554432 * 20,
            ProducerConfig.CLIENT_ID_CONFIG, this.getGROUP_ID_FOR_KAFKA(),
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.getKAFKA_BROKER(),
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class );

    private final KafkaSender< String, String > kafkaSender = KafkaSender.create(
            SenderOptions.< String, String >create( this.getGetKafkaSenderOptions().get() )
                    .maxInFlight( 1024 ) );

    private KafkaDataControl () {
        super.logging( "KafkaDataControl was created" );
    }

    public static KafkaDataControl getInstance () {
        return instance != null ? instance : ( instance = new KafkaDataControl() );
    }

    private final Supplier< Properties > setStreamProperties = () -> {
            this.getProperties().clear();
            this.getProperties().put( StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.getKAFKA_BROKER() );
            this.getProperties().put( StreamsConfig.APPLICATION_ID_CONFIG, this.getGROUP_ID_FOR_KAFKA() );
            this.getProperties().put( StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName() );
            this.getProperties().put( StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName() );
            return this.getProperties();
    };

    public void start () {
            final KStream< String, String > kStream = this.getBuilder().stream(
                    this.getRAW_GPS_LOCATION_TOPIC(),
                    Consumed.with( Serdes.String(), Serdes.String() ) );

            kStream.mapValues( values -> CassandraDataControl
                    .getInstance()
                    .getSaveCarLocation()
                    .apply( this.getGson().fromJson( values, Position.class ) ) );

            this.setKafkaStreams( new KafkaStreams( this.getBuilder().build(), this.getSetStreamProperties().get() ) );
            this.getKafkaStreams().start();
    }

    // записывает позицию от машины Эскорта
    private final Consumer< Position > writeToKafkaEscort = position -> this.getKafkaSender()
            .createOutbound()
            .send( CustomPublisher.generate( this.getTUPLE_OF_CAR_LOCATION_TOPIC(), this.getGson().toJson( position ) ) )
            .then()
            .doOnError( super::logging )
            .doOnSuccess( success -> super.logging( "Kafka got Escort car location: "
                    + position.getDeviceId()
                    + " at: " + position.getDeviceTime() ) )
            .subscribe( new CustomSubscriber<>(
                    value -> super.logging( this.getTUPLE_OF_CAR_LOCATION_TOPIC() )
            ));

    // записывает позицию от машины патрульного
    private final Consumer< Position > writeToKafkaPosition = position -> this.getKafkaSender()
            .createOutbound()
            .send( CustomPublisher.generate( this.getWEBSOCKET_SERVICE_TOPIC_FOR_ONLINE(), this.getGson().toJson( position ) ) )
            .then()
            .doOnError( super::logging )
            .doOnSuccess( success -> super.logging( "Kafka got patrul car: " + position.getDeviceId() + " at: " + position.getDeviceTime() ) )
            .subscribe( new CustomSubscriber<>(
                    value -> super.logging( this.getWEBSOCKET_SERVICE_TOPIC_FOR_ONLINE() )
            ) );

    // записывает новую машину Эскорта если она была добавлена в базу
    private final Consumer< TupleOfCar > writeToKafkaTupleOfCar = tupleOfCar ->
            this.getKafkaSender()
                    .createOutbound()
                    .send( CustomPublisher.generate( this.getNEW_TUPLE_OF_CAR_TOPIC(), this.getGson().toJson( tupleOfCar ) ) )
                    .then()
                    .doOnError( super::logging )
                    .doOnSuccess( success -> super.logging( "Kafka got TupleOfCar: " + tupleOfCar.getTrackerId() ) )
                    .subscribe( new CustomSubscriber<>(
                            value -> super.logging( this.getNEW_TUPLE_OF_CAR_TOPIC() )
                    ) );

    // записывает новую машину патрульного если она была добавлена в базу
    private final Consumer< ReqCar > writeToKafka = reqCar ->
            this.getKafkaSender()
                    .createOutbound()
                    .send( CustomPublisher.generate( this.getNEW_CAR_TOPIC(), this.getGson().toJson( reqCar ) ) )
                    .then()
                    .doOnError( super::logging )
                    .doOnSuccess( success -> super.logging( "Kafka got ReqCar: " + reqCar.getTrackerId() ) )
                    .subscribe( new CustomSubscriber<>(
                            value -> super.logging( this.getNEW_CAR_TOPIC() )
                    ) );

    public void clear () {
        instance = null;
        this.getKafkaSender().close();
        this.getKafkaStreams().close();
        super.logging( "Kafka is closed successfully" );
    }
}
