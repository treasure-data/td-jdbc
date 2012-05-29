import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

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
        Statement stmt = conn.createStatement();
        String sql = "select v from table2";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(String.valueOf(res.getObject(1)));
        }
    }
}
