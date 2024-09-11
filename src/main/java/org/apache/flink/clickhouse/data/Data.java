package org.apache.flink.clickhouse.data;

/** Parsed data. */
public interface Data {

    Object getByKey(String key);

    Object getByIndex(int index);

    int size();
}
