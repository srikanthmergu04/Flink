package com.flink.sample.process;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class PrintProcess extends ProcessFunction<String, String> {
    @Override
    public void processElement(String s, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {

        System.out.println(s);
        collector.collect(s);

    }
}
