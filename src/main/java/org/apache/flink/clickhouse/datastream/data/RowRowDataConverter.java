package org.apache.flink.clickhouse.datastream.data;

import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Map;

/** Row to RowData converter. */
public class RowRowDataConverter implements RowDataConverter<Row> {

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

    private static Object packValue(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof LocalDateTime) {
            return TimestampData.fromLocalDateTime((LocalDateTime) value);
        } else if (value instanceof String) {
            return StringData.fromString((String) value);
        } else if (value instanceof Map) {
            return new GenericMapData((Map) value);
        } else if (isArray(value)) {
            return new GenericArrayData((Object[]) value);
        } else {
            return value;
        }
    }

    public static boolean isArray(Object obj) {
        return obj != null && obj.getClass().isArray();
    }

    public static void main(String[] args) {
        Object o = packValue("");
        Object[] objects = {1, 2, 3};
        Object o1 = packValue(objects);
        Object o2 = packValue(LocalDateTime.now());
        System.out.println("RowRowDataConverter.main");
    }
}
