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

The `td-jdbc` library is based off the [`td-client-java` Java client library](https://github.com/treasure-data/td-client-java).

## Requirements

Java >= 1.6

## Install

### Install from GitHub repository

You can get latest source code using git.

    $ git clone https://github.com/treasure-data/td-jdbc.git
    $ cd td-jdbc
    $ mvn package

You will get the td-jdbc jar file in `td-jdbc/target` folder
The file name will be `td-jdbc-${jdbc.version}-jar-with-dependencies.jar`.
See the [pom.xml file](https://github.com/treasure-data/td-jdbc/blob/master/pom.xml)
for more details.

### Development

To run production tests, write your account e-mail and password to `$HOME/.td/td.conf`:
```
[account]
  user = (e-mail)
  password = (pass)
```

## Configuration

The principal options can all be provided as part of the JDBC custom URL. The
information is parsed from the URL and used to configure the `td-client-java`
library used to communicate with the Treasure Data service. See the
[`td-client-java` README](https://github.com/treasure-data/td-client-java/blob/master/README.md#configuration)
for more information.

In its simplest form the URL is composed by:

* URL prefix / custom protocol: 'jdbc:td://'
* API endpoint: e.g. 'api.treasuredata.com'
* database name: e.g. 'mydb'

For example:

    jdbc:td://api.treasuredata.com/mydb

All the above information are required. Optionally, one can use the various URL
options listed below.

### Engine Type

Specifying the 'type' parameter allows the user to select one of the optional
querying  engines Treasure Data support if the user account is enable with
capability to use such engine.

The current engines are:

* hive (default)
* pig
* impala
* presto

E.g.

    jdbc:td://api.treasuredata.com/mydb;type=impala

If the 'type' parameter is not specified, the default 'type=hive' is assumed.

### HTTPS / SSL

Specifying 'useSSL=true' in JDBC URL parameters tells the driver to communicate
with the API server using HTTPS / SSL encription. E.g.:

    jdbc:td://api.treasuredata.com/mydb;useSSL=true

SSL is off by default (useSSL=false is assumed when the parameter is not
specified).

### Proxy

If you are trying to connect from behind a proxy, you can specify the proxy
settings using the following properties:

* host: e.g. 'httpproxyhost=10.20.30.40 or 'httpproxyhost=myproxy.com'
* port: e.g. 'httpproxyport=80'

If the proxy is private (public access disabled):

* username: e.g. 'httpproxyuser=myusername'
* password: e.g. 'httpproxypassword=mypassword'

For example:

    jdbc:td://api.treasuredata.com/mydb;httpproxyhost=myproxy.com;httpproxyport=myport;httpproxyuser=myusername;httpproxypassword=mypassword


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
        Connection conn = DriverManager.getConnection(
          "jdbc:td://api.treasuredata.com/mydb",
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

When a INSERT statement is sent to the driver, the data is first buffered
in local memory. The data is uploaded into the cloud every 5 minutes.
Please note that the upload doesn't occur in realtime.

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
