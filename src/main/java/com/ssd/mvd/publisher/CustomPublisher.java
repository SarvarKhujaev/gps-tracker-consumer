package com.ssd.mvd.publisher;

import org.apache.kafka.clients.producer.ProducerRecord;

import com.ssd.mvd.kafka.kafkaConfigs.KafkaTopics;

import org.reactivestreams.Subscription;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Publisher;

public final class CustomPublisher implements Publisher< ProducerRecord< String, String > > {
    private final ProducerRecord< String, String > producerRecord;

    public static CustomPublisher generate (
            final KafkaTopics topic,
            final String message
    ) {
        return new CustomPublisher( topic, message );
    }

    private CustomPublisher (
            final KafkaTopics topic,
            final String message
    ) {
        this.producerRecord = new ProducerRecord<>( topic.getTopicName(), message );
    }

    @Override
    public void subscribe( final Subscriber subscriber ) {
        subscriber.onSubscribe( new Subscription() {
                @Override
                public void request( final long l ) {
                    subscriber.onNext( producerRecord );
                    subscriber.onComplete();
                }

                @Override
                public void cancel() {
                    subscriber.onError( new Exception( "Message was not sent!!!" ) );
                }
            }
        );
    }
}
