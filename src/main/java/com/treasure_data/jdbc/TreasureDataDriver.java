package com.treasure_data.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * @see org.apache.hadoop.hive.jdbc.HiveDriver
 */
public class TreasureDataDriver implements Driver {
    private static Logger LOG = Logger.getLogger(TreasureDataDriver.class.getName());

    /**
     * Is this driver JDBC compliant?
     */
    static final boolean JDBC_COMPLIANT = false;

    /**
     * The required prefix for the connection URL.
     */
    static final String URL_PREFIX = "jdbc:td://";

    /**
     * The required prefix for the connection URI.
     */
    static final String URI_PREFIX = "jdbc:td://";

    /**
     * If host is provided, without a port.
     */
    private static final String DEFAULT_PORT = "80";

    /**
     * Property key for the database name.
     */
    private static final String DBNAME_PROPERTY_KEY = "DBNAME";

    /**
     * Property key for the Hive Server host.
     */
    private static final String HOST_PROPERTY_KEY = "HOST";

    /**
     * Property key for the Hive Server port.
     */
    private static final String PORT_PROPERTY_KEY = "PORT";

    private static final int MAJOR_VERSION = 0;

    private static final int MINOR_VERSION = 1;

    static {
        try {
            java.sql.DriverManager.registerDriver(new TreasureDataDriver());
        } catch (SQLException e) {
            LOG.severe(String.format("%s: %s", e.getClass().getName(), e.getMessage()));
            e.printStackTrace();
        }
    }

    public TreasureDataDriver() {
        super();
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return Pattern.matches(URL_PREFIX + ".*", url);
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        return new TreasureDataConnection(url, info);
    }

    @Override
    public int getMajorVersion() {
        return MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return MINOR_VERSION;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
            throws SQLException {
        if (info == null) {
            info = new Properties();
        }

        if ((url != null) && url.startsWith(URL_PREFIX)) {
            info = parseURL(url, info);
        }

        DriverPropertyInfo hostProp = new DriverPropertyInfo(HOST_PROPERTY_KEY,
                info.getProperty(HOST_PROPERTY_KEY, ""));
        hostProp.required = false;
        hostProp.description = "Hostname of Treasure Data Cloud";

        DriverPropertyInfo portProp = new DriverPropertyInfo(PORT_PROPERTY_KEY,
                info.getProperty(PORT_PROPERTY_KEY, ""));
        portProp.required = false;
        portProp.description = "Port number of Treasure Data Cloud";

        DriverPropertyInfo dbProp = new DriverPropertyInfo(DBNAME_PROPERTY_KEY,
                info.getProperty(DBNAME_PROPERTY_KEY, "default"));
        dbProp.required = false;
        dbProp.description = "Database name";

        DriverPropertyInfo[] dpi = new DriverPropertyInfo[3];

        dpi[0] = hostProp;
        dpi[1] = portProp;
        dpi[2] = dbProp;

        return dpi;
    }

    /**
     * Takes a url in the form of jdbc:td://[hostname]:[port]/[db_name] and
     * parses it. Everything after jdbc:hive// is optional.
     * 
     * @param url
     * @param defaults
     * @return
     * @throws java.sql.SQLException
     */
    private Properties parseURL(String url, Properties defaults)
            throws SQLException {
        Properties urlProps = (defaults != null) ? new Properties(defaults) : new Properties();

        if (url == null || !url.startsWith(URL_PREFIX)) {
            throw new SQLException("Invalid connection url: " + url);
        }

        if (url.length() <= URL_PREFIX.length()) {
            return urlProps;
        }

        // [hostname]:[port]/[db_name]
        String connectionInfo = url.substring(URL_PREFIX.length());

        // [hostname]:[port] [db_name]
        String[] hostPortAndDatabase = connectionInfo.split("/", 2);

        // [hostname]:[port]
        if (hostPortAndDatabase[0].length() > 0) {
            String[] hostAndPort = hostPortAndDatabase[0].split(":", 2);
            urlProps.put(HOST_PROPERTY_KEY, hostAndPort[0]);
            if (hostAndPort.length > 1) {
                urlProps.put(PORT_PROPERTY_KEY, hostAndPort[1]);
            } else {
                urlProps.put(PORT_PROPERTY_KEY, DEFAULT_PORT);
            }
        }

        // [db_name]
        if (hostPortAndDatabase.length > 1) {
            urlProps.put(DBNAME_PROPERTY_KEY, hostPortAndDatabase[1]);
        }

        return urlProps;
    }

    @Override
    public boolean jdbcCompliant() {
        return JDBC_COMPLIANT;
    }


}
