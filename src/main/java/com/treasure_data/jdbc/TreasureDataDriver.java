package com.treasure_data.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * TreasureDataDriver.
 *
 * @see org.apache.hadoop.hive.jdbc.HiveDriver
 * @see org.apache.derby.jdbc.ClientDriver
 * @see org.hsqldb.jdbc.JDBCDriver
 */
public class TreasureDataDriver implements Driver {
    private static Logger LOG = Logger.getLogger(
            TreasureDataDriver.class.getName());

    public static TreasureDataDriver driverInstance;

    static {
        try {
            driverInstance = new TreasureDataDriver();
            DriverManager.registerDriver(driverInstance);
        } catch (Exception e) {
            LOG.severe(String.format("%s: %s",
                    e.getClass().getName(), e.getMessage()));
        }
    }

    public TreasureDataDriver() {
    }

    public Connection connect(String url, Properties props)
            throws SQLException {
        return getConnection(url, props);
    }

    public static Connection getConnection(String url, Properties props)
            throws SQLException {
        if (props == null) {
            throw new SQLException("invalid arguments: properties is null");
        }

        // a connection object is not singleton
        return new TDConnection(parseURL(url), props);
    }

    private static JDBCURLParser.Desc parseURL(String url)
            throws SQLException {
        return JDBCURLParser.parse(url);
    }

    public boolean acceptsURL(String url) throws SQLException {
        if (url == null) {
            return false;
        }

        if (url.regionMatches(true, 0, Constants.URL_PREFIX0, 0,
                Constants.URL_PREFIX0.length())) {
            return true;
        }

        return false;
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties props)
            throws SQLException {
        if (!acceptsURL(url)) {
            return new DriverPropertyInfo[0];
        }

        DriverPropertyInfo[] props0   = new DriverPropertyInfo[2];
        DriverPropertyInfo p;

        if (props == null) {
            props = new Properties();
        }

        p = new DriverPropertyInfo("user", null);
        p.value = props.getProperty("user");
        p.required = true;
        p.description = "user's email address for accessing TreasureData Cloud";
        props0[0] = p;

        p = new DriverPropertyInfo("host", null);
        p.value = props.getProperty("host");
        p.required = true;
        p.description = "host name of TreasureData Cloud";
        props0[1] = p;

        return props0;
    }

    public int getMajorVersion() {
        return Constants.DRIVER_MAJOR_VERSION;
    }

    public int getMinorVersion() {
        return Constants.DRIVER_MINOR_VERSION;
    }

    public boolean jdbcCompliant() {
        return Constants.JDBC_COMPLIANT;
    }

}
