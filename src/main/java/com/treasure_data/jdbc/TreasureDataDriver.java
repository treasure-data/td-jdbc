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
 * @see org.hsqldb.jdbcDriver
 */
public class TreasureDataDriver implements Driver, TDConstants {
    private static Logger LOG = Logger.getLogger(
            TreasureDataDriver.class.getName());

    static {
        try {
            DriverManager.registerDriver(new TreasureDataDriver());
        } catch (Exception e) {
            LOG.severe(String.format("%s: %s",
                    e.getClass().getName(), e.getMessage()));
            e.printStackTrace();
        }
    }

    public TreasureDataDriver() {
    }

    public boolean acceptsURL(String url) throws SQLException {
        return url != null && url.regionMatches(
                true, 0, URL_PREFIX0, 0, URL_PREFIX0.length());
    }

    public Connection connect(String url, Properties info)
            throws SQLException {
        return new TreasureDataConnection(url, info);
    }

    public int getMajorVersion() {
        return MAJOR_VERSION;
    }

    public int getMinorVersion() {
        return MINOR_VERSION;
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties props)
            throws SQLException {
        if (props == null) {
            props = System.getProperties();
        }

        DriverPropertyInfo[] ret = new DriverPropertyInfo[2];

        ret[0] = new DriverPropertyInfo("user", null);
        ret[0].value = props.getProperty("user", "");
        ret[0].required = true;
        ret[0].description = "user's email address for accessing TreasureData Cloud";

        ret[1] = new DriverPropertyInfo("host", null);
        ret[1].value = props.getProperty("host", "");
        ret[1].required = true;
        ret[1].description = "host name of TreasureData Cloud";

        return ret;
    }

    public boolean jdbcCompliant() {
        return JDBC_COMPLIANT;
    }

}
