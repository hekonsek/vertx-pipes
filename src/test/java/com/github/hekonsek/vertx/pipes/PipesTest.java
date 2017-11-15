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
import java.util.UUID;

import static com.github.hekonsek.vertx.pipes.internal.KafkaProducerBuilder.kafkaProducer;
import static com.github.hekonsek.vertx.pipes.Pipes.pipes;
import static io.vertx.core.Vertx.vertx;
import static io.vertx.core.json.Json.encode;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(VertxUnitRunner.class)
public class PipesTest {

    String pipeId = UUID.randomUUID().toString();

    String source = UUID.randomUUID().toString();

    Pipe pipe = Pipe.pipe(pipeId, source, "function");

    SimpleFunctionRegistry functionRegistry = new SimpleFunctionRegistry();

    Pipes pipes = pipes(vertx(), functionRegistry);

    Map<String, Object> event = ImmutableMap.of("foo", "bar");

    String eventJson = encode(event);

    Bytes eventBytes = new Bytes(eventJson.getBytes());

    @Test
    public void functionShouldReceiveEventFromPipe(TestContext context) throws InterruptedException {
        // Given
        Async async = context.async();
        functionRegistry.registerFunction("function", event -> {
            assertThat(event.body()).isEqualTo(this.event);
            async.complete();
        });
        pipes.startPipe(pipe);

        // When
        kafkaProducer(vertx()).write(new KafkaProducerRecordImpl<>(source, "key", eventBytes));
    }

}
