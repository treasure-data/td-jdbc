package com.treasure_data.jdbc;

import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;

public class TestTreasureDataConnection {

    @Test @Ignore
    public void testSimple() throws Exception {
        Properties props = new Properties();
        TreasureDataConnection conn =
            new TreasureDataConnection("jdbc:td://localhost:9999/mugadb", props);
    }
}
