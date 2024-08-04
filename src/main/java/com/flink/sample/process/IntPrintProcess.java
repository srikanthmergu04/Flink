package com.flink.sample.process;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class IntPrintProcess extends ProcessFunction<Integer, Integer> {
    @Override
    public void processElement(Integer s, ProcessFunction<Integer, Integer>.Context context, Collector<Integer> collector) throws Exception {

        System.out.println(s);
        collector.collect(s);

    }
}
