package com.flink.sample;

import com.flink.sample.process.MyBroadcastProcess;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink Hello world!
 */
public class FlinkBroadcastProcessPipeline {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .createLocalEnvironmentWithWebUI(new Configuration());

        KafkaSource<String> ruleSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("rule-events")
                .setGroupId("my-group")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSource<String> dataSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("data-events")
                .setGroupId("my-group")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        MapStateDescriptor<String, String> ruleStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<String>() {
                }));

        BroadcastStream<String> ruleBroadcastStream = env
                .fromSource(ruleSource, WatermarkStrategy.noWatermarks(), "Rule Source")
                .broadcast(ruleStateDescriptor);

        DataStreamSource<String> dataStreamSource = env
                .fromSource(dataSource, WatermarkStrategy.noWatermarks(), "Data Source");

        dataStreamSource
                .connect(ruleBroadcastStream)
                .process(new MyBroadcastProcess())
                        .setParallelism(3);

        env.setParallelism(3);

        env.execute("Flink-Hello-World");
    }
}
