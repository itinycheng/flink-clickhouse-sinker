package org.apache.flink.clickhouse.datastream.statement;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/** statement. */
public interface ClickHouseStatement<IN> extends Serializable {

    String getSql();

    void statement(PreparedStatement ps, IN record) throws SQLException;
}
