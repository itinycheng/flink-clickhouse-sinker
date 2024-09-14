package org.apache.flink.clickhouse.datastream.writer;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.StatefulSink.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink.PrecommittingSinkWriter;
import org.apache.flink.clickhouse.datastream.committer.ClickHouseCommittable;
import org.apache.flink.clickhouse.datastream.data.Row;
import org.apache.flink.clickhouse.datastream.data.RowDataConverter;
import org.apache.flink.clickhouse.datastream.metadata.ClickHouseMetadataProvider;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.clickhouse.internal.AbstractClickHouseOutputFormat;
import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseDmlOptions;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseDmlOptions.Builder;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** clickhouse writer. */
public class ClickHouseWriter<IN extends Row>
        implements StatefulSinkWriter<IN, ClickHouseWriterState>,
                PrecommittingSinkWriter<IN, ClickHouseCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseWriter.class);

    private final ClickHouseDmlOptions executionOptions;

    private final Properties connectionProperties;

    private final DeliveryGuarantee deliveryGuarantee;

    private final RowDataConverter<IN> rowDataConverter;

    private final ClickHouseConnectionProvider connectionProvider;

    private final ClickHouseMetadataProvider metadataProvider;

    private final Map<String, AbstractClickHouseOutputFormat> outputFormatMap;

    private final Map<String, String[]> columnNamesMap;

    private final int subTaskId;

    private final int numberOfSubtasks;

    public ClickHouseWriter(
            ClickHouseDmlOptions executionOptions,
            Properties connectionProperties,
            RowDataConverter<IN> rowDataConverter,
            DeliveryGuarantee deliveryGuarantee,
            Collection<ClickHouseWriterState> recoveredState,
            Sink.InitContext initContext) {
        if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
            throw new RuntimeException("EXACTLY_ONCE scenarios are not supported yet");
        }

        checkNotNull(recoveredState, "recoveredState must be defined");
        checkNotNull(initContext, "initContext must be defined");

        this.executionOptions = checkNotNull(executionOptions, "executionOptions must be defined");
        this.connectionProperties =
                checkNotNull(connectionProperties, "connectionProperties must be defined");
        this.rowDataConverter = checkNotNull(rowDataConverter, "rowDataConverter must be defined");
        this.deliveryGuarantee =
                checkNotNull(deliveryGuarantee, "deliveryGuarantee must be defined");
        this.connectionProvider =
                new ClickHouseConnectionProvider(executionOptions, connectionProperties);
        this.metadataProvider = new ClickHouseMetadataProvider(connectionProvider);
        this.outputFormatMap = new ConcurrentHashMap<>();
        this.columnNamesMap = new ConcurrentHashMap<>();

        this.subTaskId = initContext.getSubtaskId();
        this.numberOfSubtasks = initContext.getNumberOfParallelSubtasks();
        long lastCheckpointId =
                initContext
                        .getRestoredCheckpointId()
                        .orElse(Sink.InitContext.INITIAL_CHECKPOINT_ID - 1);
        LOG.info("lastCheckpointId={}", lastCheckpointId);
    }

    @Override
    public List<ClickHouseWriterState> snapshotState(long checkpointId) throws IOException {
        return Collections.emptyList();
    }

    @Override
    public Collection<ClickHouseCommittable> prepareCommit()
            throws IOException, InterruptedException {
        return Collections.emptyList();
    }

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {
        String tableName = element.table();
        if (tableName.isEmpty()) {
            return;
        }

        AbstractClickHouseOutputFormat outputFormat = outputFormatMap.get(tableName);
        if (outputFormat == null) {
            createCacheColumnsAndOutputFormat(element.database(), tableName);
        }
        outputFormat = outputFormatMap.get(tableName);
        String[] columnNames = columnNamesMap.get(tableName);
        if (outputFormat == null || columnNames == null) {
            throw new RuntimeException("outputFormat/columnNames is null");
        }

        RowData rowData = rowDataConverter.convert(element, columnNames);
        outputFormat.writeRecord(rowData);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (deliveryGuarantee != DeliveryGuarantee.EXACTLY_ONCE || endOfInput) {
            LOG.debug("final flush={}", endOfInput);
            flush();
        } else {
            outputFormatMap.values().forEach(AbstractClickHouseOutputFormat::checkFlushException);
        }
    }

    @Override
    public void close() throws Exception {
        for (AbstractClickHouseOutputFormat outputFormat : outputFormatMap.values()) {
            outputFormat.close();
        }

        connectionProvider.closeConnections();
        outputFormatMap.clear();
    }

    private void flush() throws IOException {
        for (AbstractClickHouseOutputFormat outputFormat : outputFormatMap.values()) {
            outputFormat.flush();
            outputFormat.checkFlushException();
        }
    }

    private synchronized void createCacheColumnsAndOutputFormat(String database, String table) {
        AbstractClickHouseOutputFormat outputFormat = outputFormatMap.get(table);
        if (outputFormat != null) {
            return;
        }

        try {
            ResolvedCatalogTable resolvedTable = metadataProvider.getTable(database, table);
            String[] primaryKeys =
                    resolvedTable
                            .getResolvedSchema()
                            .getPrimaryKey()
                            .map(UniqueConstraint::getColumns)
                            .map(keys -> keys.toArray(new String[0]))
                            .orElse(new String[0]);
            String[] partitionKeys = resolvedTable.getPartitionKeys().toArray(new String[0]);
            DataType physicalRowDataType =
                    resolvedTable.getResolvedSchema().toPhysicalRowDataType();
            String[] columnNames =
                    DataType.getFieldNames(physicalRowDataType).toArray(new String[0]);

            outputFormat =
                    new AbstractClickHouseOutputFormat.Builder()
                            // .withConnectionProvider(connectionProvider)
                            .withOptions(newDmlOptions(database, table))
                            .withConnectionProperties(connectionProperties)
                            .withFieldNames(columnNames)
                            .withFieldTypes(
                                    DataType.getFieldDataTypes(physicalRowDataType)
                                            .toArray(new DataType[0]))
                            .withPrimaryKey(primaryKeys)
                            .withPartitionKey(partitionKeys)
                            .build();
            outputFormat.open(subTaskId, numberOfSubtasks);

            outputFormatMap.put(table, outputFormat);
            columnNamesMap.put(table, columnNames);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to create ClickHouse OutputFormat", e);
        }
    }

    private ClickHouseDmlOptions newDmlOptions(String database, String table) {
        ClickHouseDmlOptions.Builder builder =
                new Builder()
                        .withUrl(executionOptions.getUrl())
                        .withUsername(executionOptions.getUsername().orElse(null))
                        .withPassword(executionOptions.getPassword().orElse(null))
                        .withBatchSize(executionOptions.getBatchSize())
                        .withFlushInterval(executionOptions.getFlushInterval())
                        .withMaxRetries(executionOptions.getMaxRetries())
                        .withUseLocal(executionOptions.isUseLocal())
                        .withUpdateStrategy(executionOptions.getUpdateStrategy())
                        .withShardingStrategy(executionOptions.getShardingStrategy())
                        .withUseTableDef(executionOptions.isShardingUseTableDef())
                        .withIgnoreDelete(executionOptions.isIgnoreDelete())
                        .withParallelism(executionOptions.getParallelism())
                        .withDatabaseName(database)
                        .withTableName(table);

        List<String> shardingKey = executionOptions.getShardingKey();
        if (shardingKey != null && !shardingKey.isEmpty()) {
            builder.withShardingKey(shardingKey.get(0));
        }

        return builder.build();
    }
}
