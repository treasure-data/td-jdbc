package com.treasure_data.jdbc;

import java.sql.PreparedStatement;
import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.jdbc.TDConnection;

public class TestTreasureDataConnection {

    @Test @Ignore
    public void testSimple() throws Exception {
        Properties props = System.getProperties();
        props.load(TestTreasureDataDriver.class.getClassLoader().getResourceAsStream("treasure-data.properties"));

        TDConnection conn =
            new TDConnection("jdbc:td://192.168.0.23:80/mugadb", props);
        String sql = "insert into table02 (k1, k2) values (?, ?)";
        PreparedStatement ps = conn.prepareStatement(sql);
        for (int i = 0; i < 10; i++) {
            ps.setString(1, "muga:" + i);
            ps.setInt(2, i);
            ps.execute();
        }
        System.out.println("fin");
    }
}
