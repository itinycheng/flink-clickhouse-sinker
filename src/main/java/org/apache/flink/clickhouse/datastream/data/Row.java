package org.apache.flink.clickhouse.datastream.data;

import org.apache.flink.types.RowKind;

import javax.annotation.Nonnull;

/** Row data. */
public interface Row {

    @Nonnull
    RowKind getKind();

    @Nonnull
    String table();

    @Nonnull
    String database();

    String namespace();

    Object value(String key);

    boolean contains(String key);

    int size();
}
