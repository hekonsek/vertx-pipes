package com.github.hekonsek.vertx.pipes;

import com.github.hekonsek.vertx.pipes.internal.KafkaConsumerBuilder;
import com.github.hekonsek.vertx.pipes.internal.KafkaProducerBuilder;
import com.github.hekonsek.vertx.pipes.internal.LinkedHashMapJsonCodec;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.kafka.admin.AdminUtils;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.impl.KafkaProducerRecordImpl;
import org.apache.kafka.common.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

import static io.vertx.core.buffer.Buffer.buffer;
import static io.vertx.core.json.Json.decodeValue;
import static io.vertx.core.json.Json.encodeToBuffer;

/**
 * Provides Vert.x-based data pipes abstraction over Apache Kafka. This class is a central point for managing
 * your pipes workflow.
 */
public class Pipes {

    private final static Logger LOG = LoggerFactory.getLogger(Pipes.class);

    // Constants

    public static final String HEADER_KEY = "pipes.key";

    // Collaborators

    private final Vertx vertx;

    private final FunctionRegistry functionRegistry;

    // Constructors

    public Pipes(Vertx vertx, FunctionRegistry functionRegistry) {
        this.vertx = vertx;
        this.functionRegistry = functionRegistry;

        this.vertx.eventBus().registerDefaultCodec(LinkedHashMap.class, new LinkedHashMapJsonCodec());
    }

    public static Pipes pipes(Vertx vertx, FunctionRegistry functionRegistry) {
        return new Pipes(vertx, functionRegistry);
    }

    // Operations

    public void startPipe(Pipe pipe, Handler<AsyncResult<Void>> completionHandler) {
        AdminUtils.create(vertx, "localhost:2181").createTopic(pipe.getSource(), 1, 1, done -> {
            if (done.succeeded()) {
                Function function = functionRegistry.function(pipe.getFunction());
                vertx.eventBus().consumer(pipe.getId(), function);

                KafkaProducer<String, Bytes> kafkaProducer = KafkaProducerBuilder.pipeProducer(vertx);
                KafkaConsumerBuilder.pipeConsumer(vertx, pipe.getId()).handler(record -> {
                    Map event = decodeValue(buffer(record.value().get()), Map.class);
                    DeliveryOptions headers = new DeliveryOptions().addHeader(HEADER_KEY, record.key());
                    vertx.eventBus().send(pipe.getId(), event, headers, response -> {
                        if (pipe.getTarget() != null) {
                            if (response.succeeded()) {
                                Map<String, Object> body = (Map<String, Object>) response.result().body();
                                kafkaProducer.write(new KafkaProducerRecordImpl<>(pipe.getTarget(), record.key(), new Bytes(encodeToBuffer(body).getBytes())));
                            }
                        }
                    });
                }).subscribe(pipe.getSource(), completionHandler);
            } else {
                LOG.debug("Cannot create source topic: " + pipe.getSource());
                completionHandler.handle(done);
            }
        });
    }

    public void startPipe(Pipe pipe) {
        startPipe(pipe, null);
    }

}