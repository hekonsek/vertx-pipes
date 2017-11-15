package com.github.hekonsek.vertx.pipes.internal;

import com.google.common.collect.ImmutableMap;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Bytes;

import java.util.Map;

import static java.util.UUID.randomUUID;

public class KafkaConsumerBuilder {

    public static KafkaConsumer<String, Bytes> kafkaConsumer(Vertx vertx, String groupId) {
        Map<String, String> config = ImmutableMap.of(
                "group.id", groupId,
                "bootstrap.servers", "localhost:9092",
                "value.deserializer", BytesDeserializer.class.getName(),
                "key.deserializer", StringDeserializer.class.getName());
        return KafkaConsumer.create(vertx, config);
    }

    public static KafkaConsumer<String, Bytes> kafkaConsumer(Vertx vertx) {
        return kafkaConsumer(vertx, randomUUID().toString());
    }

}
