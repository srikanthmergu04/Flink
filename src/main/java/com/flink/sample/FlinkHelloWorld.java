package com.flink.sample;

import com.flink.sample.process.IntPrintProcess;
import com.flink.sample.process.PersistToCacheProcess;
import com.flink.sample.process.PrintProcess;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("input-topic")
                .setGroupId("my-group")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaSource = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        kafkaSource
                .process(new PersistToCacheProcess())
                .setParallelism(4);

        env.setParallelism(4);

        env.execute("Flink-Hello-World");
    }
}
