package org.apache.flink.clickhouse.schema;

import org.apache.flink.util.Preconditions;

/** common table descriptor. */
public class CommonTableDescriptor implements TableDescriptor {

    private final String database;

    private final String table;

    public CommonTableDescriptor(String database, String table) {
        this.database = Preconditions.checkNotNull(database);
        this.table = Preconditions.checkNotNull(table);
    }

    @Override
    public String getDatabase() {
        return this.database;
    }

    @Override
    public String getTable() {
        return this.table;
    }
}
