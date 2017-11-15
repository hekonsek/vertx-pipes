package com.github.hekonsek.vertx.pipes;

import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;

import java.util.Map;

public interface Function extends Handler<Message<Map<String, Object>>> {

}