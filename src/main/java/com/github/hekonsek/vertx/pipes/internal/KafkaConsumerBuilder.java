package com.github.hekonsek.vertx.pipes.internal;

import com.google.common.collect.ImmutableMap;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Bytes;

import java.util.Map;

import static java.util.UUID.randomUUID;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public class KafkaConsumerBuilder {

    public static KafkaConsumer<String, Bytes> pipeConsumer(Vertx vertx, String groupId) {
        Map<String, String> config = ImmutableMap.of(
                GROUP_ID_CONFIG, groupId,
                BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName(),
                AUTO_OFFSET_RESET_CONFIG, "earliest");
        return KafkaConsumer.create(vertx, config);
    }

    public static KafkaConsumer<String, Bytes> pipeConsumer(Vertx vertx) {
        return pipeConsumer(vertx, randomUUID().toString());
    }

}
