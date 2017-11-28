package com.github.hekonsek.rxjava.pipes;

import lombok.Data;

import java.util.Map;

@Data
public class Event<T> {

    private final Map<String, Object> headers;

    private final T payload;

}
