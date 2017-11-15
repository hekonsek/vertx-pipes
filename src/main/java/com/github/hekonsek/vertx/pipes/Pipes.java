package com.github.hekonsek.vertx.pipes;

import com.github.hekonsek.vertx.pipes.internal.LinkedHashMapJsonCodec;
import com.google.common.collect.ImmutableMap;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Bytes;

import java.util.LinkedHashMap;
import java.util.Map;

public class Pipes {

    private final Vertx vertx;

    private final FunctionRegistry functionRegistry;

    public Pipes(Vertx vertx, FunctionRegistry functionRegistry) {
        this.vertx = vertx;
        this.functionRegistry = functionRegistry;
        this.vertx.eventBus().registerDefaultCodec(LinkedHashMap.class, new LinkedHashMapJsonCodec());
    }

    public static Pipes pipes(Vertx vertx, FunctionRegistry functionRegistry) {
        return new Pipes(vertx, functionRegistry);
    }

    public void startPipe(Pipe pipe) {
        Function function = functionRegistry.function(pipe.getFunction());
        vertx.eventBus().consumer(pipe.getId(), function);

        Map<String, String> consumerConfig = ImmutableMap.of(
                "group.id", pipe.getId(),
                "bootstrap.servers", "localhost:9092",
                "value.deserializer", BytesDeserializer.class.getName(),
                "key.deserializer", StringDeserializer.class.getName());
        KafkaConsumer.create(vertx, consumerConfig).handler(record -> {
            byte[] value = ((Bytes) record.value()).get();
            vertx.eventBus().send(pipe.getId(), Json.decodeValue(Buffer.buffer(value), Map.class));
        }).subscribe(pipe.getSource());
    }

}
