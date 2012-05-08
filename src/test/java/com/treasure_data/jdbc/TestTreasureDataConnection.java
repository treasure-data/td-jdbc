package com.treasure_data.jdbc;

import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.jdbc.JDBCConnection;

public class TestTreasureDataConnection {

    @Test @Ignore
    public void testSimple() throws Exception {
        Properties props = new Properties();
        JDBCConnection conn =
            new JDBCConnection("jdbc:td://localhost:9999/mugadb", props);
    }
}
