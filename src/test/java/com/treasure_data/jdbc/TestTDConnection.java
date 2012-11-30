package com.treasure_data.jdbc;

import java.sql.PreparedStatement;
import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.jdbc.TDConnection;

public class TestTDConnection {

    @Test @Ignore
    public void testSimple() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));

        TDConnection conn =
            new TDConnection(JDBCURLParser.parse("jdbc:td://192.168.0.23:80/mugadb"), props);
        String sql = "insert into table02 (k1, k2, k3) values (?, 1, ?)";
        TDPreparedStatement ps = (TDPreparedStatement) conn.prepareStatement(sql);
        for (int i = 0; i < 10; i++) {
            ps.setString(1, "muga:" + i);
            ps.setInt(2, i);
            ps.execute();
        }
        ps.getCommandExecutor().getAPI().flush();
        System.out.println("fin");
    }
}
