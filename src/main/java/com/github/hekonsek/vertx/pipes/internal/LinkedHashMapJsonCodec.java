package com.github.hekonsek.vertx.pipes.internal;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.json.Json;

import java.util.LinkedHashMap;

public class LinkedHashMapJsonCodec implements MessageCodec<LinkedHashMap, LinkedHashMap> {

    @Override public void encodeToWire(Buffer buffer, LinkedHashMap LinkedHashMap) {
        buffer.appendString(Json.encode(LinkedHashMap));
    }

    @Override public LinkedHashMap decodeFromWire(int pos, Buffer buffer) {
        return Json.decodeValue(buffer, LinkedHashMap.class);
    }

    @Override public LinkedHashMap transform(LinkedHashMap LinkedHashMap) {
        return LinkedHashMap;
    }

    @Override public String name() {
        return getClass().getSimpleName();
    }

    @Override public byte systemCodecID() {
        return -1;
    }
}