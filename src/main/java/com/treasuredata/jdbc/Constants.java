package com.treasuredata.jdbc;

public interface Constants
        extends com.treasure_data.client.Constants
{

    // The offset at which HSQLDB API Result mode values start.
    int API_BASE = 0;

    // Not a result
    int NONE = API_BASE + 0;

    // Indicates that the Result object encapsulates an update count response.
    int UPDATECOUNT = API_BASE + 1;

    // Indicates that the Result object encapsualtes an error response.
    int ERROR = API_BASE + 2;

    // Indicates that the Result object encapsulates a result set response containing data.
    int DATA = API_BASE + 3;

    /**
     * Indicates that the Result object encapsulates a response
     * that communicates the acknowlegement of newly allocated
     * Statement object in the form of its statementID
     * and metadata
     */
    int PREPARE_ACK = API_BASE + 4;

    /**
     * Indicates that the Result object encapsulates a result
     * set for setting session attributes.
     */
    int SETSESSIONATTR = API_BASE + 6;

    /**
     * Indicates that the Result object encapsulates a request
     * to get session attributes.
     */
    int GETSESSIONATTR = API_BASE + 7;

    // Indicates that the Result object encapsulates a batch of statements
    int BATCHEXECDIRECT = API_BASE + 8;

    /**
     * Indicates that the Result object encapsulates a batch of prepared
     * statement parameter values
     */
    int BATCHEXECUTE = API_BASE + 9;

    /**
     * Indicates that the Result object encapsulates a request to start a new
     * internal session for the connection
     */
    int RESETSESSION = API_BASE + 10;

    /**
     * Indicates that the Result object encapsulates a response to a connection
     * attempt that was successful
     */
    int CONNECTACKNOWLEDGE = API_BASE + 11;

    /**
     * Indicates that the Result object encapsulates a request to prepare
     * to commit as the first phase of a two-phase commit
     */
    int PREPARECOMMIT = API_BASE + 12;

    /**
     * Indicates that the Result object encapsulates a request to return
     * some rows of data
     */
    int REQUESTDATA = API_BASE + 13;

    /**
     * Indicates that the Result object encapsulates a set of data rows
     * without metadata
     */
    int DATAROWS = API_BASE + 14;

    /**
     * Indicates that the Result object encapsulates a set of data rows
     * with metadata
     */
    int DATAHEAD = API_BASE + 15;

    /**
     * Indicates that the Result object encapsulates a set of update counts
     * for a batch execution
     */
    int BATCHEXECRESPONSE = API_BASE + 16;

    // Only for metadata, indicates that the metadata is for the parameters.
    int PARAM_METADATA = API_BASE + 17;

    // Common result type for all large object operations
    int LARGE_OBJECT_OP = API_BASE + 18;

    // Warning
    int WARNING = API_BASE + 19;

    // generated data
    int GENERATED = API_BASE + 20;

    // attempt to execute invalid statement
    int EXECUTE_INVALID = API_BASE + 21;

    // Indicates that Result encapsulates a request to establish a connection.
    int CONNECT = API_BASE + 31;

    /**
     * Indicates that Result encapsulates a request to terminate an
     * established connection.
     */
    int DISCONNECT = API_BASE + 32;

    // Indicates that Result encapsulates a request to terminate an SQL-transaction.
    int ENDTRAN = API_BASE + 33;

    /**
     * Indicates that Result encapsulates a request to execute a statement
     * directly.
     */
    int EXECDIRECT = API_BASE + 34;

    /**
     * Indicates that Result encapsulates a request to execute a prepared
     * statement.
     */
    int EXECUTE = API_BASE + 35;

    /**
     * Indicates that Result encapsulates a request to deallocate an
     * SQL-statement.
     */
    int FREESTMT = API_BASE + 36;

    /**
     * Indicates that Result encapsulates a request to prepare a statement.
     */
    int PREPARE = API_BASE + 37;

    /**
     * Indicates that Result encapsulates a request to set the value of an
     * SQL-connection attribute.
     */
    int SETCONNECTATTR = API_BASE + 38;

    /**
     * Indicates that Result encapsulates a request to explicitly start an
     * SQL-transaction and set its characteristics.
     */
    int STARTTRAN = API_BASE + 39;

    // Indicates that the Result encapsulates a request to close a result set
    int CLOSE_RESULT = API_BASE + 40;

    // Indicates that the Result encapsulates a request to update or insert into a result set
    int UPDATE_RESULT = API_BASE + 41;

    // Indicates that the Result encapsulates a simple value for internal use
    int VALUE = API_BASE + 42;

    // Indicates that the Result encapsulates a response to a procedure call via CallableStatement
    int CALL_RESPONSE = API_BASE + 43;

    // Indicates that the Result encapsulates a data change set
    int CHANGE_SET = API_BASE + 44;

    // Constants above this limit are handled as non-HSQLDB results
    int MODE_UPPER_LIMIT = API_BASE + 48;

    String VOID_TYPE_NAME = "void";
    String BOOLEAN_TYPE_NAME = "boolean";
    String TINYINT_TYPE_NAME = "tinyint";
    String SMALLINT_TYPE_NAME = "smallint";
    String INT_TYPE_NAME = "int";
    String BIGINT_TYPE_NAME = "bigint";
    String FLOAT_TYPE_NAME = "float";
    String DOUBLE_TYPE_NAME = "double";
    String STRING_TYPE_NAME = "string";
    String DATE_TYPE_NAME = "date";
    String DATETIME_TYPE_NAME = "datetime";
    String TIMESTAMP_TYPE_NAME = "timestamp";
    String BINARY_TYPE_NAME = "binary";
    String LIST_TYPE_NAME = "array";
    String MAP_TYPE_NAME = "map";
    String STRUCT_TYPE_NAME = "struct";
    String UNION_TYPE_NAME = "uniontype";

    // for Presto
    String VARCHAR_TYPE_NAME = "varchar";

    int RESULT_EXECDIRECT = 0;

    // The required prefix for the connection URL and URI.
    String URL_PREFIX = "jdbc:td://";
    String URL_PREFIX0 = "jdbc:td:";
    String URI_PREFIX = URL_PREFIX;
    String DEFAULT_PORT = "80"; // If host is provided, without a port.

    String DBNAME_PROPERTY_KEY = "DBNAME"; // Property key for the database name.
    String HOST_PROPERTY_KEY = "HOST"; // Property key for the Hive Server host.
    String PORT_PROPERTY_KEY = "PORT"; // Property key for the Hive Server port.

    int MAX_ROW_SIZE = Integer.MAX_VALUE - 1;
    int MAX_STATEMENT_LENGTH = 60 * 1024;
    int MAX_TABLE_NAME_LENGTH = 128;
    int MAX_TABLES_IN_SELECT = 256;
    int MAX_USER_NAME_LENGTH = 128;
    int MAX_CATALOG_NAME_LENGTH = 128;
    int MAX_COLUMN_NAME_LENGTH = 128;
    int MAX_COLUMNS_IN_SELECT = 256;
    int MAX_COLUMNS_IN_TABLE = 512;


    String TD_JDBC_RESULT_RETRYCOUNT_THRESHOLD = "td.jdbc.result.retrycount.threshold";
    int TD_JDBC_RESULT_RETRYCOUNT_THRESHOLD_DEFAULTVALUE = 0;

    String TD_JDBC_RESULT_RETRY_WAITTIME = "td.jdbc.result.retry.waittime";
    long TD_JDBC_RESULT_RETRY_WAITTIME_DEFAULTVALUE = (20 * 1000); // millis

    // JDBC compliance
    boolean JDBC_COMPLIANT = false;

    // jdbc version
    int JDBC_MAJOR_VERSION = 3;
    int JDBC_MINOR_VERSION = 0;

    // database version
    // we define hive version as database version
    int DATABASE_MAJOR_VERSION = 0;
    int DATABASE_MINOR_VERSION = 7;
    int DATABASE_REVISION = 1;
    String DATABASE_FULL_VERSION = DATABASE_MAJOR_VERSION + "." + DATABASE_MINOR_VERSION + "." + DATABASE_REVISION;
    String DATABASE_NAME = "Treasure Data Storage Engine";

    // driver version
    int DRIVER_MAJOR_VERSION = 0;
    int DRIVER_MINOR_VERSION = 4;
    int DRIVER_REVISION = 0;
    String DRIVER_FULL_VERSION = DRIVER_MAJOR_VERSION + "." + DRIVER_MINOR_VERSION + "." + DRIVER_REVISION;
    String DRIVER_NAME = TreasureDataDriver.class.getName();
}
