package com.github.hekonsek.vertx.pipes;

import com.github.hekonsek.vertx.pipes.internal.KafkaProducerBuilder;
import com.github.hekonsek.vertx.pipes.internal.LinkedHashMapJsonCodec;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.impl.KafkaProducerRecordImpl;
import org.apache.kafka.common.utils.Bytes;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.github.hekonsek.vertx.pipes.internal.KafkaConsumerBuilder.kafkaConsumer;
import static io.vertx.core.buffer.Buffer.buffer;
import static io.vertx.core.json.Json.decodeValue;
import static io.vertx.core.json.Json.encodeToBuffer;

/**
 * Provides Vert.x-based pipes abstraction over Apache Kafka. This class is a central point for managing
 * your pipes workflow.
 */
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

        KafkaProducer<String, Bytes> kafkaProducer = KafkaProducerBuilder.kafkaProducer(vertx);
        kafkaConsumer(vertx, pipe.getId()).handler(record -> {
            byte[] eventBytes = record.value().get();
            vertx.eventBus().send(pipe.getId(), decodeValue(buffer(eventBytes), Map.class), response -> {
                if(pipe.getTarget() != null) {
                    if (response.succeeded()) {
                        Map<String, Object> body = (Map<String, Object>) response.result().body();
                        kafkaProducer.write(new KafkaProducerRecordImpl<>(pipe.getTarget(), record.key(), new Bytes(encodeToBuffer(body).getBytes())));
                    }
                }
            });
        }).subscribe(pipe.getSource());
    }

}
