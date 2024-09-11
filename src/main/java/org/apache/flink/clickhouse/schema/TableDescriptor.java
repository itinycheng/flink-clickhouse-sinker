package org.apache.flink.clickhouse.schema;

/** table name. */
public interface TableDescriptor {

    String getDatabase();

    String getTable();
}
