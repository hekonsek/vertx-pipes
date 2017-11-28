package com.github.hekonsek.rxjava.pipes;

import com.google.common.collect.ImmutableMap;
import io.debezium.kafka.KafkaCluster;
import io.vertx.core.json.Json;
import io.vertx.reactivex.kafka.client.producer.KafkaProducerRecord;
import org.apache.kafka.common.utils.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static com.github.hekonsek.rxjava.pipes.KafkaProducerBuilder.pipeProducer;
import static com.google.common.io.Files.createTempDir;
import static io.vertx.reactivex.core.Vertx.vertx;
import static java.util.UUID.randomUUID;

public class RxJavaPipesTest {

    static {
        try {
            new KafkaCluster().withPorts(2181, 9092).usingDirectory(createTempDir()).deleteDataPriorToStartup(true).addBrokers(1).startup();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    String topic = randomUUID().toString();

    @Test
    public void should() throws InterruptedException {
        new KafkaSource<String, Map>(vertx(), topic).build().
                map(event ->  event.getPayload().get("foo")).
                subscribe(System.out::println);

        Bytes event = new Bytes(Json.encode(ImmutableMap.of("foo", "bar")).getBytes());
        pipeProducer(vertx()).rxWrite(KafkaProducerRecord.create(topic, "key", event)).
                subscribe(result -> System.out.println(result.toJson()));

        Thread.sleep(5000);
    }

}
