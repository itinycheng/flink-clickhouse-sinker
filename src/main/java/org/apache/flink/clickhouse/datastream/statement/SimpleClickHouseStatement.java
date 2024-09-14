package org.apache.flink.clickhouse.datastream.statement;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** statement. */
public class SimpleClickHouseStatement<IN> implements ClickHouseStatement<IN> {

    private final String sql;

    private final ClickHouseStatementBuilder<IN> statementBuilder;

    public SimpleClickHouseStatement(String sql, ClickHouseStatementBuilder<IN> statementBuilder) {
        this.sql = checkNotNull(sql, "sql cannot be null");
        this.statementBuilder = checkNotNull(statementBuilder, "statementBuilder cannot be null");
    }

    public String getSql() {
        return sql;
    }

    public void statement(PreparedStatement ps, IN record) throws SQLException {
        statementBuilder.accept(ps, record);
    }
}
