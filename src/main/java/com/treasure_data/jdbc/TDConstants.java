package com.treasure_data.jdbc;

public interface TDConstants {
    /**
     * Is this driver JDBC compliant?
     */
    boolean JDBC_COMPLIANT = false;

    /**
     * The required prefix for the connection URL.
     */
    String URL_PREFIX = "jdbc:td://";

    String URL_PREFIX0 = "jdbc:td:";

    /**
     * The required prefix for the connection URI.
     */
    String URI_PREFIX = "jdbc:td://";

    /**
     * If host is provided, without a port.
     */
    String DEFAULT_PORT = "80";

    /**
     * Property key for the database name.
     */
    String DBNAME_PROPERTY_KEY = "DBNAME";

    /**
     * Property key for the Hive Server host.
     */
    String HOST_PROPERTY_KEY = "HOST";

    /**
     * Property key for the Hive Server port.
     */
    String PORT_PROPERTY_KEY = "PORT";

    int MAJOR_VERSION = 0;

    int MINOR_VERSION = 1;

    String FULL_VERSION = MAJOR_VERSION + "." + MINOR_VERSION;

    String DRIVER_NAME = TreasureDataDriver.class.getName();

    int MAJOR_VERSION_DATABASE = 0;

    int MINOR_VERSION_DATABASE = 0;

    String FULL_VERSION_DATABASE = MAJOR_VERSION_DATABASE + "." + MINOR_VERSION_DATABASE;

    String DATABASE_NAME = "td-hadoop";
}
