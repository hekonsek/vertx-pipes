package com.github.hekonsek.vertx.pipes;

import lombok.Data;

import java.util.Map;

import static java.util.Collections.emptyMap;

@Data
public class Pipe {

    private final String id;

    private final String source;

    private final String function;

    private final String target;

    private final Map<String, Object> configuration;

    public static Pipe pipe(String id, String source, String function) {
        return new Pipe(id, source, function, null, emptyMap());
    }

}