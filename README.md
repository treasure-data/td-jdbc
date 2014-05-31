# Treasure Data JDBC Driver

## Overview

Many web/mobile applications generate huge amount of event logs (c,f. login,
logout, purchase, follow, etc).  Analyzing these event logs can be quite
valuable for improving services.  However, collecting these logs easily and 
reliably is a challenging task.

This driver enables you to use Treasure Data with a standard JDBC interface.

  * Treasure Data website: [http://treasure-data.com/](http://treasure-data.com/)
  * Treasure Data GitHub: [https://github.com/treasure-data/](https://github.com/treasure-data/)

The following link is how to use the JDBC driver.

  * Treasure Data JDBC Driver: [http://docs.treasure-data.com/articles/jdbc-driver](http://docs.treasure-data.com/articles/jdbc-driver)

## Requirements

Java >= 1.6

## Install

### Install from GitHub repository

You can get latest source code using git.

    $ git clone https://github.com/treasure-data/td-jdbc.git
    $ cd td-jdbc
    $ mvn package

You will get the td-jdbc jar file in td-jdbc/target
directory.  File name will be td-jdbc-${jdbc.version}-jar-with-dependencies.jar.
For more detail, see pom.xml.

## Quickstart

The following program is a small example of the JDBC Driver.

    import java.io.IOException;
    import java.sql.Connection;
    import java.sql.DriverManager;
    import java.sql.ResultSet;
    import java.sql.Statement;
    import java.util.Properties;
    import com.treasure_data.jdbc.TreasureDataDriver;

    public class JDBCSample {
      public static void loadSystemProperties() throws IOException {
        Properties props = System.getProperties();
        props.load(TreasureDataDriver.class.getClassLoader().getResourceAsStream("treasure-data.properties"));
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
          "jdbc:td://api.treasuredata.com/testdb",
          "YOUR_MAIL_ADDRESS_HERE",
          "YOUR_PASSWORD_HERE");
        Statement stmt = conn.createStatement();
        String sql = "SELECT count(1) FROM www_access";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
          System.out.println(String.valueOf(res.getObject(1)));
        }
      }
    }

When a SELECT statement is sent to the driver, the driver will issue the
query to the cloud. The driver will regularly poll the job results while
the jobs run on the cloud. The query may take several hours, we recommend
that you use a background thread.

You specify useSSL=true in JDBC URL as parameter, you can use SSL for
connecting our API server like following:

    jdbc:td://api.treasuredata.com/testdb;useSSL=true


When a INSERT statement is sent to the driver, the data is first buffered
in local memory. The data is uploaded into the cloud every 5 minutes.
Please note that the upload doesn’t occur in realtime.

## Implementation Status

Following methods have been implemented already.

### java.sql.Connection

  * createStatement() and createStatement(..)
  * getMetaData()
  * prepareStatement(..)

### java.sql.Statement

  * execute(..)
  * executeQuery(..)
  * setResultSet()

### java.sql.PreparedStatement

  * addBatch()
  * clearParameters()
  * execute()
  * executeQuery()
  * getMetaData()
  * setBoolean(..)
  * setByte(..)
  * setDouble(..)
  * setFloat(..)
  * setInt(..)
  * setLong(..)
  * setShort(..)
  * setString(..)

### java.sql.ResultSet

  * findColumn(String)
  * getBoolean(..)
  * getByte(..)
  * getDate(int) and getDate(String)
  * getDouble(int)
  * getFloat(int)
  * getInt(int)
  * getLong(int)
  * getMetaData()
  * next()
  * getObject(int) and getObject(String)
  * getShort(..)

### java.sql.ResultSetMetaData

  * getColumnCount()
  * getColumnDisplaySize(int)
  * getColumnLabel(int)
  * getColumnName(int)
  * getColumnType(int)
  * getColumnTypeName(int)

### java.sql.DatabaseMetaData

  * getCatalogs()
  * getColumns(..)
  * getSchemas(..)
  * getTableTypes()
  * getTables(..)

## License

Apache License, Version 2.0
