package org.apache.flink.clickhouse.data;

import java.util.Properties;

/** Data parser. */
public interface DataParser<T> {

    /** open parser. */
    default void open(Properties props) throws Exception {}

    /** is valid data. */
    boolean isValid(String name, T value);

    /** parse data. */
    Data parse(String name, T value) throws Exception;

    default void close() throws Exception {}
}
