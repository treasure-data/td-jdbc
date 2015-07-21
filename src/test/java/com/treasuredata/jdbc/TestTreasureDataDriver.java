package com.treasuredata.jdbc;

import org.junit.Ignore;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

@Ignore
public class TestTreasureDataDriver
{

    public static void loadSystemProperties()
            throws IOException
    {
        Properties props = System.getProperties();
        props.load(TestTreasureDataDriver.class.getClassLoader().getResourceAsStream("treasure-data.properties"));
    }

    public static void main0(String[] args)
            throws Exception
    {
        loadSystemProperties();

        final String driverName = "com.treasure_data.jdbc.TreasureDataDriver";
        try {
            Class.forName(driverName);
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        Connection conn = DriverManager.getConnection(
                "jdbc:td://localhost:9999/mugadb", "", "");
        Statement stmt = conn.createStatement();

        String sql = "select count(*) from mugatbl";
        //String sql = "select v['uid'] from mugatbl";
        //String sql = "select * from mugatbl";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(String.valueOf(res.getObject(1)));
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        loadSystemProperties();

        final String driverName = "com.treasure_data.jdbc.TreasureDataDriver";
        try {
            Class.forName(driverName);
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        Connection conn = DriverManager.getConnection(
                "jdbc:td://192.168.0.23:80/mugadb", "", "");
        Statement stmt = conn.createStatement();

        String sql = "select v from loggertable";
        //String sql = "select v['uid'] from mugatbl";
        //String sql = "select * from mugatbl";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(String.valueOf(res.getObject(1)));
        }
    }
}
