package org.apache.flink.clickhouse;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.clickhouse.data.Data;
import org.apache.flink.clickhouse.schema.TableDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

/** sinker. */
public class DynamicClickHouseSinkFunction extends RichSinkFunction<Tuple2<TableDescriptor, Data>>
        implements CheckpointedFunction {

    private Map<TableDescriptor, String> insertSqlCache;

    private transient ScheduledExecutorService executor;

    private transient ScheduledFuture<?> scheduledFuture;

    private transient long numPendingRequests;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void invoke(Tuple2<TableDescriptor, Data> value, Context context) throws Exception {
        super.invoke(value, context);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {}

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext)
            throws Exception {}
}
