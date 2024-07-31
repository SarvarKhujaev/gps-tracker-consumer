package com.ssd.mvd.interfaces;

import com.ssd.mvd.kafka.kafkaConfigs.KafkaTopics;

/*
хранит все методы нужные для объектов которые отправляются через Кафку
*/
public interface KafkaEntitiesCommonMethods {
    KafkaTopics getTopicName();

    String getSuccessMessage();

    default String generateMessage() {
        return "Kafka got request for topic: " + this.getTopicName();
    }
}
