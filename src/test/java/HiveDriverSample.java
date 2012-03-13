import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Ignore;

@Ignore
public class HiveDriverSample {
    public static void main(String[] args) throws Exception {
	String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";

	try {
	    Class.forName(driverName);
	} catch (ClassNotFoundException e) {
	    e.printStackTrace();
	    System.exit(1);
	}

	Connection con = DriverManager.getConnection(
		"jdbc:hive://localhost:10000/default", "", "");
	Statement stmt = con.createStatement();

	String sql = "select count(1) from table";
	System.out.println("Running: " + sql);
	ResultSet res = stmt.executeQuery(sql);
	while (res.next()) {
	    System.out.println(String.valueOf(res.getString(1)));
	}
    }
}
