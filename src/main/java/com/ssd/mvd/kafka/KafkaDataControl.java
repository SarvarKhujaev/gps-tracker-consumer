package com.ssd.mvd.kafka;

import java.util.*;
import lombok.Data;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.ssd.mvd.entity.ReqCar;
import com.ssd.mvd.entity.Position;
import com.ssd.mvd.entity.TupleOfCar;
import com.ssd.mvd.GpsTrackerApplication;
import com.ssd.mvd.database.CassandraDataControl;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Consumed;

import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;

@Data
public class KafkaDataControl extends SerDes {
    private Properties properties = new Properties();
    private static KafkaDataControl instance = new KafkaDataControl();

    private final String KAFKA_BROKER = GpsTrackerApplication
            .context
            .getEnvironment()
            .getProperty( "variables.KAFKA_BROKER" );

    private final String GROUP_ID_FOR_KAFKA = GpsTrackerApplication
            .context
            .getEnvironment()
            .getProperty( "variables.GROUP_ID_FOR_KAFKA" );

    private final String NEW_TUPLE_OF_CAR_TOPIC = GpsTrackerApplication
            .context
            .getEnvironment()
            .getProperty( "variables.NEW_TUPLE_OF_CAR_TOPIC" );

    private final String TUPLE_OF_CAR_LOCATION_TOPIC = GpsTrackerApplication
            .context
            .getEnvironment()
            .getProperty( "variables.TUPLE_OF_CAR_LOCATION_TOPIC" );

    private final String NEW_CAR_TOPIC = GpsTrackerApplication
            .context
            .getEnvironment()
            .getProperty( "variables.NEW_CAR_TOPIC" );

    private final String WEBSOCKET_SERVICE_TOPIC_FOR_ONLINE = GpsTrackerApplication
            .context
            .getEnvironment()
            .getProperty( "variables.WEBSOCKET_SERVICE_TOPIC_FOR_ONLINE" );

    private final String RAW_GPS_LOCATION_TOPIC = GpsTrackerApplication
            .context
            .getEnvironment()
            .getProperty( "variables.RAW_GPS_LOCATION_TOPIC" );

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

    private KafkaDataControl () { super.logging( "KafkaDataControl was created" ); }

    public static KafkaDataControl getInstance () { return instance != null ? instance : ( instance = new KafkaDataControl() ); }

    private final Supplier< Properties > setStreamProperties = () -> {
        this.getProperties().clear();
        this.getProperties().put( StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.getKAFKA_BROKER() );
        this.getProperties().put( StreamsConfig.APPLICATION_ID_CONFIG, this.getGROUP_ID_FOR_KAFKA() );
        this.getProperties().put( StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName() );
        this.getProperties().put( StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName() );
        return this.getProperties(); };

    public void start () {
        final KStream< String, String > kStream = this.getBuilder().stream( this.getRAW_GPS_LOCATION_TOPIC(),
                Consumed.with( Serdes.String(), Serdes.String() ) );

        kStream.mapValues( values -> CassandraDataControl
                .getInstance()
                .getAddPosition()
                .apply( super.deserialize( values ) ) );

        this.setKafkaStreams( new KafkaStreams( this.getBuilder().build(), this.getSetStreamProperties().get() ) );
        this.getKafkaStreams().start(); }

    private final Consumer< Position > writeToKafkaEscort = position -> this.getKafkaSender()
            .createOutbound()
            .send( Mono.just( new ProducerRecord<>( this.getTUPLE_OF_CAR_LOCATION_TOPIC(), super.serialize( position ) ) ) )
            .then()
            .doOnError( super::logging )
            .doOnSuccess( success -> super.logging(
                    "Kafka got Escort car location: "
                            + position.getDeviceId()
                            + " at: " + position.getDeviceTime() ) )
            .subscribe();

    private final Consumer< Position > writeToKafkaPosition = position -> this.getKafkaSender()
            .createOutbound()
            .send( Mono.just( new ProducerRecord<>( this.getWEBSOCKET_SERVICE_TOPIC_FOR_ONLINE(), super.serialize( position ) ) ) )
            .then()
            .doOnError( super::logging )
            .doOnSuccess( success -> super.logging(
                    "Kafka got: " + position.getDeviceId() +
                            " at: " + position.getDeviceTime() ) )
            .subscribe();

    private final Function< TupleOfCar, TupleOfCar > writeToKafkaTupleOfCar = tupleOfCar -> {
        this.getKafkaSender()
                .createOutbound()
                .send( Mono.just( new ProducerRecord<>( this.getNEW_TUPLE_OF_CAR_TOPIC(), super.serialize( tupleOfCar ) ) ) )
                .then()
                .doOnError( super::logging )
                .doOnSuccess( success -> super.logging( "Kafka got TupleOfCar: " + tupleOfCar.getTrackerId() ) )
                .subscribe();
        return tupleOfCar; };

    private final Function< ReqCar, ReqCar > writeToKafka = reqCar -> {
        this.getKafkaSender()
                .createOutbound()
                .send( Mono.just( new ProducerRecord<>( this.getNEW_CAR_TOPIC(), super.serialize( reqCar ) ) ) )
                .then()
                .doOnError( super::logging )
                .doOnSuccess( success -> super.logging( "Kafka got ReqCar: " + reqCar.getTrackerId() ) )
                .subscribe();
        return reqCar; };

    public void clear () {
        CassandraDataControl.getInstance().clear();
        super.logging( "Kafka was closed" );
        this.getProperties().clear();
        this.setProperties( null );
        instance = null;
        super.stop(); }
}
