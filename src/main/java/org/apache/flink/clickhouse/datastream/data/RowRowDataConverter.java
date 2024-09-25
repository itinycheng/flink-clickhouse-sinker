package org.apache.flink.clickhouse.datastream.data;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Row to RowData converter. */
public abstract class RowRowDataConverter implements RowDataConverter<Row> {

    private static final Logger LOG = LoggerFactory.getLogger(RowRowDataConverter.class);

    @Override
    public RowData convert(Row record, String[] fieldNames) {
        GenericRowData rowData = new GenericRowData(record.size());
        for (int i = 0; i < fieldNames.length; i++) {
            if (!record.contains(fieldNames[i])) {
                LOG.debug("Field {} not found in record", fieldNames[i]);
            }

            Object value = record.value(fieldNames[i]);
            rowData.setField(i, packValue(value));
        }

        rowData.setRowKind(record.getKind());
        return rowData;
    }

    protected abstract Object packValue(Object value);
}
