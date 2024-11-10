package com.flink.sample.process;


import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class MyBroadcastProcess extends BroadcastProcessFunction<String, String, String> {
    @Override
    public void processElement(String string, BroadcastProcessFunction<String, String, String>.ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {
        System.out.println("processElement");
    }

    @Override
    public void processBroadcastElement(String string, BroadcastProcessFunction<String, String, String>.Context context, Collector<String> collector) throws Exception {
        System.out.println("processBroadcastElement");
    }
}
