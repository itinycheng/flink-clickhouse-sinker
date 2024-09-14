package org.apache.flink.clickhouse.datastream.metadata;

/** table name. */
public interface TableDescriptor {

    String getDatabase();

    String getTable();
}
