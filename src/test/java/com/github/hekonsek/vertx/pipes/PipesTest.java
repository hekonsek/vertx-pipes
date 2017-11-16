package com.github.hekonsek.vertx.pipes;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.debezium.kafka.KafkaCluster;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.producer.impl.KafkaProducerRecordImpl;
import org.apache.kafka.common.utils.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Map;

import static com.github.hekonsek.vertx.pipes.Pipes.pipes;
import static com.github.hekonsek.vertx.pipes.internal.KafkaConsumerBuilder.pipeConsumer;
import static com.github.hekonsek.vertx.pipes.internal.KafkaProducerBuilder.pipeProducer;
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

    String key = randomUUID().toString();

    Map<String, Object> event = ImmutableMap.of("foo", "bar");

    String eventJson = encode(event);

    Bytes eventBytes = new Bytes(eventJson.getBytes());

    static {
        try {
            new KafkaCluster().withPorts(2181, 9092).usingDirectory(Files.createTempDir()).deleteDataPriorToStartup(true).addBrokers(1).startup();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test(timeout = 5000)
    public void functionShouldReceiveEventFromPipe(TestContext context) {
        Async async = context.async();
        functionRegistry.registerFunction("function", event -> {
            assertThat(event.body()).isEqualTo(this.event);
            async.complete();
        });
        pipes.startPipe(sinkPipe, event ->  {
            pipeProducer(vertx()).write(new KafkaProducerRecordImpl<>(source, "key", eventBytes));
        });
    }

    @Test(timeout = 5000)
    public void functionShouldReceiveEventKey(TestContext context) {
        Async async = context.async();
        functionRegistry.registerFunction("function", event -> {
            assertThat(event.headers().get(Pipes.HEADER_KEY)).isEqualTo(key);
            async.complete();
        });
        pipes.startPipe(sinkPipe, event -> {
            pipeProducer(vertx()).write(new KafkaProducerRecordImpl<>(source, key, eventBytes));
        });
    }

    @Test(timeout = 5000)
    public void shouldWriteToTargetTopic(TestContext context) {
        Async async = context.async();
        functionRegistry.registerFunction("function", event -> event.reply(event.body()));
        pipes.startPipe(pipe, event -> {
            pipeProducer(vertx()).write(new KafkaProducerRecordImpl<>(source, "key", eventBytes));
            pipeConsumer(vertx()).handler(event2 -> async.complete()).subscribe(target);
        });
    }

}