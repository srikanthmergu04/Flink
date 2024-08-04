package com.flink.sample;

import com.flink.sample.process.IntPrintProcess;
import com.flink.sample.process.PrintProcess;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * Flink Hello world!
 */
public class FlinkHelloWorld {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);

        env.fromElements("Hello, World")
                .process(new PrintProcess());

        env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
                .process(new IntPrintProcess());


        env.execute("Fraud Detection");

    }
}
