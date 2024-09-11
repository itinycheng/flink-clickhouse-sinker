package org.apache.flink.clickhouse.data;

import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

/** An implementation of {@link Data} which wraps the parsed data. */
public class ParsedData implements Data {

    private final String[] header;

    private final Object[] data;

    private final Map<String, Integer> headerMap;

    public ParsedData(String[] header, Object[] data) {
        Preconditions.checkNotNull(header);
        Preconditions.checkNotNull(data);
        Preconditions.checkState(
                header.length == data.length, "header length must be equal to data length");

        this.header = header;
        this.data = data;
        this.headerMap = new HashMap<>(header.length);
        for (int i = 0; i < header.length; i++) {
            headerMap.put(header[i], i);
        }
    }

    @Override
    public Object getByKey(String key) {
        Integer i = headerMap.get(key);
        if (i == null || i >= data.length) {
            return null;
        }
        return data[i];
    }

    @Override
    public Object getByIndex(int index) {
        if (index >= data.length) {
            return null;
        }
        return data[index];
    }

    @Override
    public int size() {
        return data.length;
    }
}
