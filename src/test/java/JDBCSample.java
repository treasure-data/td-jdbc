import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import com.treasure_data.jdbc.TDPreparedStatement;
import com.treasure_data.logger.TreasureDataLogger;

public class JDBCSample {
    public static void loadSystemProperties() throws IOException {
        Properties props = System.getProperties();
        props.load(JDBCSample.class.getClassLoader().getResourceAsStream("treasure-data.properties"));
    }

    public static String getSystemProperty(String key) {
        Properties props = System.getProperties();
        return props.getProperty(key);
    }

    public static void main(String[] args) throws Exception {
        loadSystemProperties();

        try {
            Class.forName("com.treasure_data.jdbc.TreasureDataDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        String user = getSystemProperty("td.api.user");
        String password = getSystemProperty("td.api.password");
        Connection conn = DriverManager.getConnection(
                //"jdbc:td://192.168.0.23:80/mugadb", user, password);
                "jdbc:td://api.treasure-data.com:80/mugatest", user, password);
        String sql = "insert into table4 (time) values (?)";
        TDPreparedStatement stmt = (TDPreparedStatement) conn.prepareStatement(sql);
        for (int i = 0; i < 10; i++) {
            stmt.setString(1, "muga:" + i);
            stmt.addBatch();
        }
        stmt.executeBatch();

        System.out.println("Running: " + stmt);
        TreasureDataLogger.flushAll();
    }

    public static void main2(String[] args) throws Exception {
        loadSystemProperties();

        try {
            Class.forName("com.treasure_data.jdbc.TreasureDataDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        String user = getSystemProperty("td.api.user");
        String password = getSystemProperty("td.api.password");
        Connection conn = DriverManager.getConnection(
                //"jdbc:td://192.168.0.23:80/mugadb", user, password);
                "jdbc:td://api.treasure-data.com:80/mugadb", user, password);
        Statement stmt = conn.createStatement();
        String sql = "select v['score'] as score, v['id'] as id, v['name'] as name from score order by score desc";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(String.valueOf(res.getObject(1)));
        }
    }
}
