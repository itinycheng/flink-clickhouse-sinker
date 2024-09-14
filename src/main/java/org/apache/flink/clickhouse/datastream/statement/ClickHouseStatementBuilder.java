package org.apache.flink.clickhouse.datastream.statement;

import org.apache.flink.util.function.BiConsumerWithException;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/** ClickHouse statement builder. */
public class ClickHouseStatementBuilder<IN>
        implements BiConsumerWithException<PreparedStatement, IN, SQLException>, Serializable {

    @Override
    public void accept(PreparedStatement statement, IN in) throws SQLException {
        // TODO
    }
}
