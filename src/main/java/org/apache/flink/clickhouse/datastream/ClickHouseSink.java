package org.apache.flink.clickhouse.datastream;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.clickhouse.datastream.committer.ClickHouseCommittable;
import org.apache.flink.clickhouse.datastream.committer.ClickHouseCommittableSerializer;
import org.apache.flink.clickhouse.datastream.committer.ClickHouseCommitter;
import org.apache.flink.clickhouse.datastream.data.Row;
import org.apache.flink.clickhouse.datastream.data.RowDataConverter;
import org.apache.flink.clickhouse.datastream.writer.ClickHouseWriter;
import org.apache.flink.clickhouse.datastream.writer.ClickHouseWriterState;
import org.apache.flink.clickhouse.datastream.writer.ClickHouseWriterStateSerializer;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseDmlOptions;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** clickhouse sink. */
public class ClickHouseSink<IN extends Row>
        implements StatefulSink<IN, ClickHouseWriterState>,
                TwoPhaseCommittingSink<IN, ClickHouseCommittable>
/*, SupportsConcurrentExecutionAttempts*/ {

    private final DeliveryGuarantee deliveryGuarantee;

    private final ClickHouseDmlOptions executionOptions;

    private final Properties connectionProperties;

    private final ClickHouseConnectionProvider connectionProvider;

    private final RowDataConverter<IN> converter;

    public ClickHouseSink(
            DeliveryGuarantee deliveryGuarantee,
            ClickHouseDmlOptions executionOptions,
            Properties connectionProperties,
            RowDataConverter<IN> converter) {
        this.deliveryGuarantee =
                checkNotNull(deliveryGuarantee, "deliveryGuarantee cannot be null");
        this.connectionProperties =
                checkNotNull(connectionProperties, "connectionProvider cannot be null");
        this.executionOptions = checkNotNull(executionOptions, "executionOptions cannot be null");
        this.connectionProvider =
                new ClickHouseConnectionProvider(executionOptions, connectionProperties);
        this.converter = checkNotNull(converter, "statement cannot be null");
    }

    public static <IN extends Row> ClickHouseSinkBuilder<IN> builder() {
        return new ClickHouseSinkBuilder<>();
    }

    @Override
    public ClickHouseWriter<IN> createWriter(InitContext context) throws IOException {
        return restoreWriter(context, Collections.emptyList());
    }

    @Override
    public ClickHouseWriter<IN> restoreWriter(
            InitContext context, Collection<ClickHouseWriterState> recoveredState) {
        return new ClickHouseWriter<>(
                executionOptions,
                connectionProperties,
                converter,
                deliveryGuarantee,
                recoveredState,
                context);
    }

    @Override
    public Committer<ClickHouseCommittable> createCommitter() {
        return new ClickHouseCommitter(deliveryGuarantee, connectionProvider);
    }

    @Override
    public SimpleVersionedSerializer<ClickHouseCommittable> getCommittableSerializer() {
        return new ClickHouseCommittableSerializer();
    }

    @Override
    public SimpleVersionedSerializer<ClickHouseWriterState> getWriterStateSerializer() {
        return new ClickHouseWriterStateSerializer();
    }
}
