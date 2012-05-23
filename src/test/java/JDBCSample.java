import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import com.treasure_data.jdbc.TestTreasureDataDriver;

public class JDBCSample {
    public static void loadSystemProperties() throws IOException {
        Properties props = System.getProperties();
        props.load(TestTreasureDataDriver.class.getClassLoader().getResourceAsStream("treasure-data.properties"));
    }

    public static void main(String[] args) throws Exception {
        loadSystemProperties();

        try {
            Class.forName("com.treasure_data.jdbc.TreasureDataDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        Connection conn = DriverManager.getConnection(
                "jdbc:td://192.168.0.23:80/mugadb", "", "");
        Statement stmt = conn.createStatement();
        String sql = "select v from loggertable";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(String.valueOf(res.getObject(1)));
        }
    }
}
