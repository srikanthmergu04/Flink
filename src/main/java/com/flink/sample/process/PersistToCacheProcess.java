package com.flink.sample.process;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Map;

public class PersistToCacheProcess extends ProcessFunction<String, String> implements CheckpointedFunction {

    ListState<Map> mapListState = null;

    @Override
    public void processElement(String str, Context context, Collector<String> collector) throws Exception {

        try {
            Map<String, Object> value = new ObjectMapper().readValue(str, Map.class);
            System.out.println(value);
            mapListState.add(value);

            Iterator<Map> iterator = mapListState.get().iterator();

            System.out.println("-----------------");
            while (iterator.hasNext()) {
                System.out.println(iterator.next());
            }
            System.out.println("-----------------");

            collector.collect(str);
        } catch (Exception exception) {
            System.out.println("Error : " + exception.getMessage());
        }

    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        ListState<Map> listState = functionInitializationContext
                .getOperatorStateStore()
                .getListState(new ListStateDescriptor<>("listState", Map.class));
        mapListState = listState;

    }
}
