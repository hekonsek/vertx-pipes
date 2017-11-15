package com.github.hekonsek.vertx.pipes.internal;

import com.google.common.collect.ImmutableMap;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;

import java.util.Map;

public class KafkaProducerBuilder {

    public static KafkaProducer<String, Bytes> kafkaProducer(Vertx vertx) {
        Map<String, String> consumerConfig = ImmutableMap.of(
                "bootstrap.servers", "localhost:9092",
                "value.serializer", BytesSerializer.class.getName(),
                "key.serializer", StringSerializer.class.getName());
        return KafkaProducer.create(vertx, consumerConfig);
    }

}
