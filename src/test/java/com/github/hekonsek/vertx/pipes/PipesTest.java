package com.github.hekonsek.vertx.pipes;

import com.google.common.collect.ImmutableMap;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.producer.impl.KafkaProducerRecordImpl;
import org.apache.kafka.common.utils.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Map;

import static com.github.hekonsek.vertx.pipes.Pipes.pipes;
import static com.github.hekonsek.vertx.pipes.internal.KafkaConsumerBuilder.kafkaConsumer;
import static com.github.hekonsek.vertx.pipes.internal.KafkaProducerBuilder.kafkaProducer;
import static io.vertx.core.Vertx.vertx;
import static io.vertx.core.json.Json.encode;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(VertxUnitRunner.class)
public class PipesTest {

    String pipeId = randomUUID().toString();

    String source = randomUUID().toString();

    Pipe sinkPipe = Pipe.pipe(pipeId, source, "function");

    String target = randomUUID().toString();

    Pipe pipe = Pipe.pipe(pipeId, source, "function", target);

    SimpleFunctionRegistry functionRegistry = new SimpleFunctionRegistry();

    Pipes pipes = pipes(vertx(), functionRegistry);

    Map<String, Object> event = ImmutableMap.of("foo", "bar");

    String eventJson = encode(event);

    Bytes eventBytes = new Bytes(eventJson.getBytes());

    @Test
    public void functionShouldReceiveEventFromPipe(TestContext context) {
        // Given
        Async async = context.async();
        functionRegistry.registerFunction("function", event -> {
            assertThat(event.body()).isEqualTo(this.event);
            async.complete();
        });
        pipes.startPipe(sinkPipe);

        // When
        kafkaProducer(vertx()).write(new KafkaProducerRecordImpl<>(source, "key", eventBytes));
    }

    @Test
    public void shouldWriteToTargetTopic(TestContext context) {
        // Given
        Async async = context.async();
        functionRegistry.registerFunction("function", event -> event.reply(event.body()));
        pipes.startPipe(pipe);

        // When
        kafkaProducer(vertx()).write(new KafkaProducerRecordImpl<>(source, "key", eventBytes));

        // Then
        kafkaConsumer(vertx()).handler(event -> async.complete()).subscribe(target);
    }

}
