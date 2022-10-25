package com.ssd.mvd.kafka;

import lombok.Data;
import lombok.NonNull;

import java.util.*;
import java.time.LocalDateTime;
import java.util.logging.Logger;
import java.util.concurrent.ExecutionException;

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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Data
public class KafkaDataControl {
    private Properties properties;
    private final AdminClient client;
    private final KafkaTemplate< String, String > kafkaTemplate;
    private static KafkaDataControl instance = new KafkaDataControl();
    private final Logger logger = Logger.getLogger( KafkaDataControl.class.toString() );

    private final String KAFKA_BROKER = GpsTrackerApplication
            .context
            .getEnvironment()
            .getProperty( "variables.KAFKA_BROKER" );

    private final String ID = GpsTrackerApplication
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

    private final Supplier< Properties > setProperties = () -> {
        this.setProperties( new Properties() );
        this.getProperties().put( AdminClientConfig.CLIENT_ID_CONFIG, this.ID );
        this.getProperties().put( AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.KAFKA_BROKER);
        return getProperties(); };
    private final Supplier< Properties > setStreamProperties = () -> {
        this.getProperties().clear();
        this.getProperties().put( StreamsConfig.APPLICATION_ID_CONFIG, this.getID() );
        this.getProperties().put( StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.getKAFKA_BROKER() );
        this.getProperties().put( StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName() );
        this.getProperties().put( StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName() );
        return this.getProperties(); };

    public static KafkaDataControl getInstance () { return instance != null ? instance : ( instance = new KafkaDataControl() ); }

    private final Consumer< String > getNewTopic = imei -> {
        try { if ( !this.getClient().listTopics().names().get().contains( imei ) ) {
            this.logger.info( "Topic: " + imei + " was created" );
            this.getClient().createTopics( Collections.singleton(
                    TopicBuilder
                            .name( imei )
                            .partitions( 5 )
                            .replicas( 3 )
                            .build() ) );
        } } catch ( ExecutionException | InterruptedException e ) { throw new RuntimeException(e); } };

    private KafkaTemplate< String, String > kafkaTemplate () {
        Map< String, Object > map = new HashMap<>();
        map.put( ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.KAFKA_BROKER);
        map.put( ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class );
        map.put( ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class );
        return new KafkaTemplate<>( new DefaultKafkaProducerFactory<>( map ) ); }

    private KafkaDataControl () {
        this.kafkaTemplate = this.kafkaTemplate();
        this.logger.info( "KafkaDataControl was created" );
        this.client = KafkaAdminClient.create( this.getSetProperties().get() );
        this.getGetNewTopic().accept( this.getWEBSOCKET_SERVICE_TOPIC_FOR_ONLINE() );
        this.getGetNewTopic().accept( this.getRAW_GPS_LOCATION_TOPIC() );
        this.getGetNewTopic().accept( this.getTUPLE_OF_CAR_LOCATION_TOPIC() );
        this.getGetNewTopic().accept( this.getNEW_TUPLE_OF_CAR_TOPIC() );
        this.getGetNewTopic().accept( this.getNEW_CAR_TOPIC() ); }

    public void start () {
        KStream< String, String > kStream = this.getBuilder().stream( this.getRAW_GPS_LOCATION_TOPIC(),
                Consumed.with( Serdes.String(), Serdes.String() ) );
        kStream.mapValues( values -> CassandraDataControl
                .getInstance()
                .getAddPosition()
                .apply( SerDes.getSerDes().deserialize( values ) ) );

        this.setKafkaStreams( new KafkaStreams( this.getBuilder().build(), this.getSetStreamProperties().get() ) );
        this.getKafkaStreams().start(); }

    private final Consumer< Position > writeToKafkaEscort = position -> this.getKafkaTemplate().send(
            this.getTUPLE_OF_CAR_LOCATION_TOPIC(), SerDes.getSerDes().serialize( position ) )
            .addCallback( new ListenableFutureCallback<>() {
                @Override
                public void onFailure( @NonNull Throwable ex ) {
                    logger.warning( "Kafka does not work since: receive"
                            + LocalDateTime.now() );
                    clear(); }

                @Override
                public void onSuccess( SendResult< String, String > result ) {
//                    logger.info( "Kafka got Escort car location: " + position.getDeviceId() +
//                            " at: " + position.getDeviceTime() +
//                            " with offset: " + result.getRecordMetadata().offset() );
                } } );

    private final Consumer< Position > writeToKafkaPosition = position -> this.getKafkaTemplate().send(
            this.getWEBSOCKET_SERVICE_TOPIC_FOR_ONLINE(), SerDes.getSerDes().serialize( position ) )
            .addCallback( new ListenableFutureCallback<>() {
                @Override
                public void onFailure( @NonNull Throwable ex ) { logger.warning( "Kafka does not work since: receive"
                        + LocalDateTime.now() );
                    clear(); }

                @Override
                public void onSuccess( SendResult< String, String > result ) {
//                    logger.info( "Kafka got: " + position.getDeviceId() +
//                            " at: " + position.getDeviceTime() +
//                            " with offset: " + result.getRecordMetadata().offset() );
                } } );

    private final Function< ReqCar, ReqCar > writeToKafka = reqCar -> {
        this.getKafkaTemplate().send( this.getNEW_CAR_TOPIC(), SerDes.getSerDes().serialize( reqCar ) )
                .addCallback( new ListenableFutureCallback<>() {
                    @Override
                    public void onFailure( @NonNull Throwable ex ) { logger.warning( "Kafka does not work since: receive"
                            + LocalDateTime.now() );
                        clear(); }

                    @Override
                    public void onSuccess( SendResult< String, String > result ) {
                        logger.info( "Kafka got ReqCar: " + reqCar.getTrackerId() +
                                " with offset: " + result.getRecordMetadata().offset() ); } } );
        return reqCar; };
    private final Function< TupleOfCar, TupleOfCar > writeToKafkaTupleOfCar = tupleOfCar -> {
        this.getKafkaTemplate().send( this.getNEW_TUPLE_OF_CAR_TOPIC(), SerDes.getSerDes().serialize( tupleOfCar ) )
                .addCallback( new ListenableFutureCallback<>() {
                    @Override
                    public void onFailure( @NonNull Throwable ex ) { logger.warning( "Kafka does not work since: receive" + LocalDateTime.now() ); }

                    @Override
                    public void onSuccess( SendResult< String, String > result ) {
                        logger.info( "Kafka got TupleOfCar " + tupleOfCar.getTrackerId() +
                                " with offset: " + result.getRecordMetadata().offset() );
                        clear(); } } );
        return tupleOfCar; };

    public void clear () {
        this.logger.info( "Kafka was closed" );
        CassandraDataControl.getInstance().clear();
        this.getKafkaTemplate().destroy();
        this.getKafkaTemplate().flush();
        Inspector.getInspector().stop();
        this.getProperties().clear();
        this.setProperties( null );
        this.getClient().close();
        instance = null; }
}
