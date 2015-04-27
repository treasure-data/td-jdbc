package com.treasure_data.jdbc;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;


import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestTDConnection {

    private static Properties loadProperties() throws IOException {
        Properties props = System.getProperties();
        props.load(TestTDConnection.class.getClassLoader().getResourceAsStream("treasure-data.properties"));
        return props;
    }

    @Test @Ignore
    public void testSimple() throws Exception {
        Properties props = loadProperties();
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

    @Test
    public void testProxy() throws Exception {
        Properties props = new Properties();
        props.setProperty("user", "leo@treasure-data.com");
        props.setProperty("password", "(password)");
        TDConnection conn = new TDConnection(JDBCURLParser.parse("jdbc:td://api.treasuredata.com/leodb;httpproxyhost=52.68.59.109;" +
            "type=presto;database=leodb;httpproxyport=3128;httpproxyuser=test;httpproxypassword=test"), props);
        try {
            String sql = "select 1";
            Statement st = conn.createStatement();
            boolean success = st.execute("select count(1) from presto_benchmark");
            assertTrue(success);

            ResultSet rs = st.getResultSet();
            assertTrue(rs.next());
            int v = rs.getInt(1);
            assertEquals(7467, v);
            assertFalse(rs.next());
        }
        finally {
            conn.close();
        }

    }

}
