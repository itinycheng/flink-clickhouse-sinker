package org.apache.flink;

import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

/** Unit test for simple App. */
public class JdbcTest {

    private Connection connection;

    @Before
    public void before() throws Exception {
        String url = "jdbc:clickhouse://localhost:8123/default";
        Properties properties = new Properties();
        properties.setProperty("user", "admin");
        properties.setProperty("password", "emZY3L5y");

        connection = DriverManager.getConnection(url, properties);
        System.out.println("Connected to: " + connection.getMetaData().getURL());
    }

    @Test
    public void testInsert() throws Exception {
        System.out.println("Inserting data...");
        try (PreparedStatement statement =
                connection.prepareStatement(
                        "INSERT INTO default.test_tuple3(`arr.a`, `arr.b`, `z`) VALUES (?, ?, ?)")) {
            statement.setArray(1, connection.createArrayOf("Int32", new Integer[] {1, 2, 3}));
            statement.setArray(2, connection.createArrayOf("Int32", new Integer[] {4, 5, 6}));
            statement.addBatch();
            statement.executeBatch();
        }
    }
}
