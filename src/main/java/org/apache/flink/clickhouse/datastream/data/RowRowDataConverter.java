package org.apache.flink.clickhouse.datastream.data;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Row to RowData converter. */
public abstract class RowRowDataConverter<IN extends Row> implements RowDataConverter<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(RowRowDataConverter.class);

    @Override
    public RowData convert(IN record, String[] fieldNames) {
        GenericRowData rowData = new GenericRowData(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            if (!record.contains(fieldNames[i])) {
                LOG.debug("Field {} not found in record", fieldNames[i]);
            }

            Object value = record.value(fieldNames[i]);
            rowData.setField(i, boxValue(value));
        }

        rowData.setRowKind(record.getKind());
        return rowData;
    }

    protected abstract Object boxValue(Object value);
}
