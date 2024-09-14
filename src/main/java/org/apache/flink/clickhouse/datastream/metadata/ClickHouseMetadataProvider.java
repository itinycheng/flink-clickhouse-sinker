package org.apache.flink.clickhouse.datastream.metadata;

import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import org.apache.flink.connector.clickhouse.util.DataTypeUtil;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseResultSetMetaData;
import com.clickhouse.jdbc.ClickHouseStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Table metadata provider. */
public class ClickHouseMetadataProvider {

    private static final Logger log = LoggerFactory.getLogger(ClickHouseMetadataProvider.class);
    private final ClickHouseConnection connection;

    public ClickHouseMetadataProvider(ClickHouseConnectionProvider connectionProvider) {
        Preconditions.checkNotNull(connectionProvider);
        try {
            this.connection = connectionProvider.getOrCreateConnection();
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to get/create ClickHouse connection", e);
        }
    }

    public ResolvedCatalogTable getTable(String databaseName, String tableName)
            throws SQLException {
        if (!tableExists(databaseName, tableName)) {
            throw new FlinkRuntimeException(
                    String.format("Table %s.%s does not exist", databaseName, tableName));
        }

        ResolvedSchema resolvedSchema = createTableSchema(databaseName, tableName);
        return new ResolvedCatalogTable(
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        null,
                        getPartitionKeys(databaseName, tableName),
                        Collections.emptyMap()),
                resolvedSchema);
    }

    private boolean tableExists(String databaseName, String tableName) throws SQLException {
        try (ClickHouseStatement stmt = connection.createStatement();
                ResultSet rs =
                        stmt.executeQuery(
                                String.format(
                                        "SELECT 1 from `system`.tables where `database` = '%s' and `name` = '%s'",
                                        databaseName, tableName))) {
            return rs.next();
        } catch (SQLException e) {
            log.error("Failed to check if table exists", e);
            return false;
        }
    }

    private ResolvedSchema createTableSchema(String databaseName, String tableName)
            throws SQLException {
        try (ClickHouseStatement stmt = connection.createStatement();
                ResultSet rs =
                        stmt.executeQuery(
                                String.format(
                                        "SELECT * from `%s`.`%s` limit 0",
                                        databaseName, tableName))) {
            ClickHouseResultSetMetaData metaData =
                    rs.getMetaData().unwrap(ClickHouseResultSetMetaData.class);
            Method getColMethod = metaData.getClass().getDeclaredMethod("getColumn", int.class);
            getColMethod.setAccessible(true);

            List<String> primaryKeys = getPrimaryKeys(databaseName, tableName);

            List<Column> columns = new ArrayList<>();
            for (int idx = 1; idx <= metaData.getColumnCount(); idx++) {
                ClickHouseColumn columnInfo = (ClickHouseColumn) getColMethod.invoke(metaData, idx);
                String columnName = columnInfo.getColumnName();
                DataType columnType = DataTypeUtil.toFlinkType(columnInfo);
                if (primaryKeys.contains(columnName)) {
                    columnType = columnType.notNull();
                }
                columns.add(Column.physical(columnName, columnType));
            }

            return new ResolvedSchema(
                    columns,
                    Collections.emptyList(),
                    !primaryKeys.isEmpty() ? UniqueConstraint.primaryKey("pk", primaryKeys) : null);
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    String.format("Failed to get the columns of %s.%s", databaseName, tableName),
                    e);
        }
    }

    private List<String> getPrimaryKeys(String databaseName, String tableName) throws SQLException {
        try (PreparedStatement stmt =
                        connection.prepareStatement(
                                String.format(
                                        "SELECT name from `system`.columns where `database` = '%s' and `table` = '%s' and is_in_primary_key = 1",
                                        databaseName, tableName));
                ResultSet rs = stmt.executeQuery()) {
            List<String> primaryKeys = new ArrayList<>();
            while (rs.next()) {
                primaryKeys.add(rs.getString(1));
            }

            return primaryKeys;
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Failed getting primary keys of table %s.%s", databaseName, tableName),
                    e);
        }
    }

    private List<String> getPartitionKeys(String databaseName, String tableName)
            throws SQLException {
        try (PreparedStatement stmt =
                        connection.prepareStatement(
                                String.format(
                                        "SELECT name from `system`.columns where `database` = '%s' and `table` = '%s' and is_in_partition_key = 1",
                                        databaseName, tableName));
                ResultSet rs = stmt.executeQuery()) {
            List<String> partitionKeys = new ArrayList<>();
            while (rs.next()) {
                partitionKeys.add(rs.getString(1));
            }

            return partitionKeys;
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    String.format("Failed to get partition keys of %s.%s", databaseName, tableName),
                    e);
        }
    }
}
