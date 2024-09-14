package org.apache.flink.clickhouse.datastream.data;

import org.apache.flink.table.data.RowData;

import java.io.Serializable;

/** row data converter. */
public interface RowDataConverter<IN> extends Serializable {

    RowData convert(IN record, String[] fieldNames);
}
