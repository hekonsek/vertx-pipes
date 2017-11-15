package com.github.hekonsek.vertx.pipes;

import java.util.LinkedHashMap;
import java.util.Map;

public class SimpleFunctionRegistry implements FunctionRegistry {

    private final Map<String, Function> functions = new LinkedHashMap<>();

    public void registerFunction(String functionName, Function function) {
        if(function instanceof StartableFunction) {
            ((StartableFunction) function).start();
        }
        functions.put(functionName, function);
    }

    @Override public Function function(String functionName) {
        return functions.get(functionName);
    }

}
