package com.ssd.mvd.interfaces;

import com.ssd.mvd.kafka.kafkaConfigs.KafkaTopics;

@SuppressWarnings(
        value = "хранит все методы нужные для объектов которые отправляются через Кафку"
)
public interface KafkaEntitiesCommonMethods {
    @lombok.NonNull
    KafkaTopics getTopicName();

    @lombok.NonNull
    String getSuccessMessage();

    @lombok.NonNull
    default String generateMessage() {
        return "Kafka got request for topic: " + this.getTopicName();
    }
}
