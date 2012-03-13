import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Ignore;

@Ignore
public class JDBCSample {
    public static void main(String[] args) {
	derbyMain(args);
    }

    public static void derbyMain(String[] args) {
	try {
	    Class.forName("org.apache.derby.jdbc.ClientDriver").newInstance();
	    Connection con = DriverManager.getConnection(
		    "jdbc:derby://localhost:1527/seconddb");
	    Statement stmt = con.createStatement();
	    ResultSet rs = stmt.executeQuery("SELECT * from secondtable");
	    while (rs.next()) {
		System.out.println(rs.getString("name"));
	    }
	    rs.close();
	    stmt.close();
	    con.close();

	} catch (SQLException e) {
	    e.printStackTrace();
	    System.out.println("SQLException: " + e.getMessage());
	    System.out.println("    SQLState: " + e.getSQLState());
	    System.out.println(" VendorError: " + e.getErrorCode());
	} catch (Exception e) {
	    e.printStackTrace();
	    System.out.println("Exception: " + e.getMessage());
	}
    }

    public static void mysqlMain(String[] args) {
	try {
	    Class.forName("com.mysql.jdbc.Driver").newInstance();
	    Connection con = DriverManager.getConnection(
		    //"jdbc:mysql://localhost/test", "root", "passwd");
		    "jdbc:mysql://localhost/phppro", "root", "");
	    Statement stmt = con.createStatement();
	    ResultSet rs = stmt.executeQuery("SELECT * from kohaku");
	    while (rs.next()) {
		System.out.println(rs.getString("aka_tori"));
	    }
	    rs.close();
	    stmt.close();
	    con.close();

	} catch (SQLException e) {
	    System.out.println("SQLException: " + e.getMessage());
	    System.out.println("    SQLState: " + e.getSQLState());
	    System.out.println(" VendorError: " + e.getErrorCode());
	} catch (Exception e) {
	    System.out.println("Exception: " + e.getMessage());
	}
    }
}
