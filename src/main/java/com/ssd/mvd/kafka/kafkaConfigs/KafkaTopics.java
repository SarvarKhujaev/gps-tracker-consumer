package com.ssd.mvd.kafka.kafkaConfigs;

import com.ssd.mvd.constants.Errors;
import com.ssd.mvd.inspectors.DataValidateInspector;

public enum KafkaTopics {
    NEW_TUPLE_OF_CAR_TOPIC {
        @Override
        public String getTopicName () {
            return DataValidateInspector
                    .getInstance()
                    .checkContextOrReturnDefaultValue(
                            "variables.KAFKA_VARIABLES.KAFKA_TOPICS.NEW_TUPLE_OF_CAR_TOPIC",
                            Errors.DATA_NOT_FOUND.name()
                    );
        }
    },

    TUPLE_OF_CAR_LOCATION_TOPIC {
        @Override
        public String getTopicName () {
            return DataValidateInspector
                    .getInstance()
                    .checkContextOrReturnDefaultValue(
                            "variables.KAFKA_VARIABLES.KAFKA_TOPICS.TUPLE_OF_CAR_LOCATION_TOPIC",
                            Errors.DATA_NOT_FOUND.name()
                    );
        }
    },

    NEW_CAR_TOPIC {
        @Override
        public String getTopicName () {
            return DataValidateInspector
                    .getInstance()
                    .checkContextOrReturnDefaultValue(
                            "variables.KAFKA_VARIABLES.KAFKA_TOPICS.NEW_CAR_TOPIC",
                            Errors.DATA_NOT_FOUND.name()
                    );
        }
    },

    WEBSOCKET_SERVICE_TOPIC_FOR_ONLINE {
        @Override
        public String getTopicName () {
            return DataValidateInspector
                    .getInstance()
                    .checkContextOrReturnDefaultValue(
                            "variables.KAFKA_VARIABLES.KAFKA_TOPICS.WEBSOCKET_SERVICE_TOPIC_FOR_ONLINE",
                            Errors.DATA_NOT_FOUND.name()
                    );
        }
    },

    RAW_GPS_LOCATION_TOPIC;

    public String getTopicName () {
        return DataValidateInspector
                .getInstance()
                .checkContextOrReturnDefaultValue(
                        "variables.KAFKA_VARIABLES.KAFKA_TOPICS.RAW_GPS_LOCATION_TOPIC",
                        Errors.DATA_NOT_FOUND.name()
                );
    }
}
