package org.apache.flink.clickhouse.datastream;

import org.apache.flink.clickhouse.datastream.data.Row;
import org.apache.flink.clickhouse.datastream.data.RowDataConverter;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseDmlOptions;

import java.util.Properties;

/** sink builder. */
public class ClickHouseSinkBuilder<IN extends Row> {

    private static final String CATALOG_NAME = "clickhouse";

    private DeliveryGuarantee deliveryGuarantee;

    private ClickHouseDmlOptions executionOptions;

    private Properties connectionProperties;

    private RowDataConverter<IN> converter;

    public ClickHouseSinkBuilder<IN> withDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
        this.deliveryGuarantee = deliveryGuarantee;
        return this;
    }

    public ClickHouseSinkBuilder<IN> withOptions(ClickHouseDmlOptions executionOptions) {
        this.executionOptions = executionOptions;
        return this;
    }

    public ClickHouseSinkBuilder<IN> withConnectionProperties(Properties connectionProperties) {
        this.connectionProperties = connectionProperties;
        return this;
    }

    public ClickHouseSinkBuilder<IN> withRowDataConverter(RowDataConverter<IN> converter) {
        this.converter = converter;
        return this;
    }

    public ClickHouseSink<IN> build() {
        return new ClickHouseSink<>(
                deliveryGuarantee, executionOptions, connectionProperties, converter);
    }
}
