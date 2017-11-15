package com.github.hekonsek.vertx.pipes;

import io.vertx.core.eventbus.Message;

import java.util.Map;

public interface EventExpression<T> {

    T evaluate(Message<Map<String, Object>> event);

}