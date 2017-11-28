package com.github.hekonsek.rxjava.pipes;

import com.google.common.collect.ImmutableMap;
import io.reactivex.Observable;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;

import static com.github.hekonsek.rxjava.pipes.KafkaEventAdapter.stringAndBytesToMap;
import static java.util.UUID.randomUUID;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public class KafkaSource<K, V> implements Source<V> {

    private final Vertx vertx;

    private final String topic;

    private String groupId = randomUUID().toString();

    private KafkaEventAdapter<K, V> eventAdapter = (KafkaEventAdapter<K, V>) stringAndBytesToMap();

    public KafkaSource(Vertx vertx, String topic) {
        this.vertx = vertx;
        this.topic = topic;
    }

    public Observable<Event<V>> build() {
        Map<String, String> config = ImmutableMap.of(
                GROUP_ID_CONFIG, groupId,
                BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                KEY_DESERIALIZER_CLASS_CONFIG, eventAdapter.getKeyDeserializer().getName(),
                VALUE_DESERIALIZER_CLASS_CONFIG, eventAdapter.getValueDeserializer().getName(),
                AUTO_OFFSET_RESET_CONFIG, "earliest");
        return KafkaConsumer.<K, V>create(vertx, config).subscribe(topic).
                toObservable().map(eventAdapter.getMapping());
    }

    public KafkaSource groupId(String groupId) {
        this.groupId = groupId;
        return this;
    }

}