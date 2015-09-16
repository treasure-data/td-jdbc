# Treasure Data JDBC Driver

JDBC Driver for accessing [Treasure Data](http://www.treasuredata.com).

- Driver class name: `com.treasuredata.jdbc.TreasureDataDriver`
- Works with Java 1.6 or higher
- [Genral Usage](http://docs.treasure-data.com/articles/jdbc-driver)
- td-jdbc internally uses [`td-client-java`](https://github.com/treasure-data/td-client-java),
a Java client for Treasure Data.

## For Maven Users

```
<dependency>
  <groupId>com.treasuredata</groupId>
  <artifactId>td-jdbc</artifactId>
  <version>0.5.1</version>
</dependency>
```

# Usage

Create a `java.sql.Connection` object using JDBC address `jdbc:td://api.treasuredata.com/(database name)`.

* [JDBC connection example](src/test/java/com/treasuredata/jdbc/Example.java)

```java
Properties props = new Properties();
props.setProperty("user", "(your account e-mail)");
props.setProperty("password", "(your password)");

// Alternatively, you can use API key instead of user and password
// props.setProperty("apikey", "(your API key)")

// Set the other options

// Use SSL (default) or not
// props.setProperty("useSSL", "true");

// Run Hive jobs. The default is "presto"
// props.setProperty("type", "hive");

// proxy configurarion (optional)
// props.setProperty("httpproxyhost", "(proxy host)");
// props.setProperty("httpproxyport", "(proxy port)");
// props.setProperty("httpproxyuser", "(proxy username)");
// props.setProperty("httpproxypassword", "(proxy password)");

Connection conn = DriverManager.getConnection("jdbc:td://api.treasuredata.com/sample_datasets", props);
Statement st = conn.createStatement();
try {
    ResultSet rs = st.executeQuery("SELECT count(1) FROM www_access");
    while (rs.next()) {
        int count = rs.getInt(1);
        System.out.println("result = " + count);
    }
    rs.close();
}
finally {
    st.close();
    conn.close();
}
```

To configure td-jdbc connection parameters, use URL parameters, Properties object or System properties. The precedence of these properties is:

1. Environment variable (only for TD_API_KEY parameter)
1. System properties
1. Properties object passed by `DriverManager.getConnection(jdbc_url, Properties)`
1. JDBC URL parameters (e.g., `jdbc:td://api.treasuredata.com/mydb;type=hive;useSSL=true`), separated by semi-colon `;`

If your environment defines TD_API_KEY variable, td-jdbc uses it. For the other properties, System properties have the highest priority.

## A list of JDBC Configurations

You must provide `apikey` property or both `user` (your account e-mail) and `password` for the authentication:

|key     | default value | description |
|--------|---------------|-------------|
|`apikey`  |               | API key to access Treasure Data. You can also set this via TD_API_KEY environment variable.  |
|`user`    |               | Account e-mail address (unnecessary if `apikey` is set) |
|`password`|               | Account password (unnecessary if `apikey` is set) |
|`type`    | presto        | Query engine. hive, preto or pig |
|`useSSL`  | false         | Use SSL encryption for accessing Treasure Data |
|`httpproxyhost` |         | Proxy host (optional) e.g., "myproxy.com"  |
|`httpproxyport`|         | Proxy port (optional) e.g., "80" |
|`httpproxyuser` |         | Proxy user (optional)  |
|`httpproxypassword` |     | Proxy password (optional)  |

If both `user` and `password` are given, td-jdbc uses this pair instead of `apikey`. 

You can also use [td-client-java specific options](https://github.com/treasure-data/td-client-java/blob/master/README.md#configuration).

# Internals

When running a query (e.g. SELECT), the driver submits a job request to
Treasure Data. td-jdbc periodically monitors the job progress and fetches the
result after the job completion.

For INSERT statement, td-jdbc buffers the data into local memory,
then flushes it to Treasure Data every 5 minutes, so there will be a delay
until your data becomes accessible in Treasure Data.

## Implementation Status

Following methods have been implemented.

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

## For developers

### Building

You can get latest source code using git.

    $ git clone git@github.com/treasure-data/td-jdbc.git
    $ cd td-jdbc
    $ mvn package

You will get the td-jdbc jar file in `td-jdbc/target` folder
The file name will be `td-jdbc-${jdbc.version}-jar-with-dependencies.jar`.
See the [pom.xml file](https://github.com/treasure-data/td-jdbc/blob/master/pom.xml)
for more details.

To run production tests, write your apikey to `$HOME/.td/td.conf`:
```
[account]
  user = (e-mail address)
  apikey = (apikey)
  password = (password)
```

### Buidling td-jdbc with JDK7 or higher

jdbc-api-4.1.jar, which is contained in mvn-local, is necessary to build td-jdbc using an older version (4.1) of JDBC API.

#### Building jdbc-api-4.1.jar on Mac OS X

- Install jdk6 https://support.apple.com/kb/DL1572?locale=en_US
```
$ jar xvf jar xvf /System/Library/Java/JavaVirtualMachines/1.6.0.jdk/Contents/Home/bundle/Classes/classes.jar java/sql javax/sql
$ jar cvf jdbc-api-4.1.jar java javax
$ mvn deploy:deploy-file -Durl=file://(path to td-jdbc folder)/mvn-local -Dfile=jdbc-api-4.1.jar -DgroupId=com.treasuredata.thirdparty -DartifactId=jdbc-api -Dpackaging=jar -Dversion=4.1
``
