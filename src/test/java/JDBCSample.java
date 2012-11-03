import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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

    public static void insertSample() throws Exception {
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

    public static void selectSample() throws Exception {
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
                "jdbc:td://api.treasure-data.com:80/mugadb", user, password);
        Statement stmt = conn.createStatement();
        stmt.setQueryTimeout(5);
        String sql = "select v['score'] as score, v['id'] as id, v['name'] as name from score order by score desc";
        //String sql = "select v from score";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        try {
            while (res.next()) {
                //System.out.println(String.valueOf(res.getObject(1)));
                System.out.println(res.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            res.close();
        }
    }

    public static void dataabseMetadataSample() throws Exception {
        //loadSystemProperties();

        try {
            Class.forName("com.treasure_data.jdbc.TreasureDataDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        String user = getSystemProperty("td.api.user");
        String password = getSystemProperty("td.api.password");
        Connection conn = DriverManager.getConnection(
                "jdbc:td://localhost:9999/mugadb", "k@treasure-data.com", "treasure311");
                //"jdbc:td://api.treasure-data.com:80/mugadb", user, password);
        DatabaseMetaData dmd = conn.getMetaData();
        ResultSet rs = dmd.getColumns(null, null, "score", null);
        while (rs.next()) {
            System.out.println(rs.getString("TABLE_NAME"));
        }
    }

    public static void main(String[] args) throws Exception {
        selectSample();
    }
}
