package com.treasure_data.jdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;

import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.model.Database;

public class JDBCConnection implements Connection, Constants {

    private TreasureDataClient client;

    private Database database;

    private SQLWarning warnings = null;

    public JDBCConnection(String uri, Properties props)
            throws SQLException {
        if (uri == null || uri.isEmpty() || !uri.startsWith(URI_PREFIX)) {
            throw new SQLException("Invalid URL: " + uri);
        }

        // remove prefix
        uri = uri.substring(Constants.URI_PREFIX.length());
        if (uri.isEmpty()) {
            throw new SQLException("Error accessing Treasure Data Cloud");
        }

        // parse uri form: host:port/db
        String[] fragments = uri.split("/");
        String[] hostport = fragments[0].split(":");
        int port = 80;
        String host = hostport[0];
        try {
            port = Integer.parseInt(hostport[1]);
        } catch (Exception e) {
            // *ignore*: if an exception is thrown, 80 is
            // inserted into a port variable. 
        }
        props.setProperty(Config.TD_API_SERVER_HOST, host + ":" + port);

        if (fragments.length > 1) {
            database = new Database(fragments[1]);
        } else {
            throw new SQLException(
            "Can't create a connection because database is not specified");
        }

        client = new TreasureDataClient(props);
    }

    public void clearWarnings() throws SQLException {
        warnings = null;
    }

    public SQLWarning getWarnings() throws SQLException {
        return warnings;
    }

    public void setWarning(SQLWarning w) throws SQLException {
        if (w == null) {
            throw new SQLException("a SQLWarning object is null");
        }

        if (warnings == null) {
            warnings = w;
        } else {
            warnings.setNextWarning(w);
        }
    }

    public void close() throws SQLException {
    }

    public boolean isClosed() throws SQLException {
        return true;
    }

    public void commit() throws SQLException {
        throw new SQLException("Method not supported");
    }

    public Array createArrayOf(String arg0, Object[] arg1) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public Blob createBlob() throws SQLException {
        throw new SQLException("Method not supported");
    }

    public Clob createClob() throws SQLException {
        throw new SQLException("Method not supported");
    }

    public NClob createNClob() throws SQLException {
        throw new SQLException("Method not supported");
    }

    public SQLXML createSQLXML() throws SQLException {
        throw new SQLException("Method not supported");
    }

    /**
     * Creates a Statement object for sending SQL statements to
     * the database.
     * 
     * @throws SQLException
     *           if a database access error occurs.
     * @see java.sql.Connection#createStatement()
     */
    public Statement createStatement() throws SQLException {
        // *skip* checking if database is null or not. It is because
        // it was processed when creating a connection.
        return new JDBCStatement(client, database); // TODO
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return new JDBCStatement(client, database); // TODO
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        return new JDBCStatement(client, database); // TODO
    }

    public Struct createStruct(String typeName, Object[] attributes)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    public boolean getAutoCommit() throws SQLException {
        return true;
    }

    public String getCatalog() throws SQLException {
        return "";
    }

    public Properties getClientInfo() throws SQLException {
        throw new SQLException("Method not supported");
    }

    public String getClientInfo(String name) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public int getHoldability() throws SQLException {
        throw new SQLException("Method not supported");
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        return new TreasureDataDatabaseMetaData(client);
    }

    public int getTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLException("Method not supported");
    }

    public boolean isReadOnly() throws SQLException {
        return false;
    }

    public boolean isValid(int timeout) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public String nativeSQL(String sql) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public CallableStatement prepareCall(String sql, int resultSetType,
            int resultSetConcurrency) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public CallableStatement prepareCall(String sql, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new TreasureDataPreparedStatement(client, database, sql); // TODO
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
            throws SQLException {
        return new TreasureDataPreparedStatement(client, database, sql); // TODO
    }

    public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
            throws SQLException {
        return new TreasureDataPreparedStatement(client, database, sql); // TODO
    }

    public PreparedStatement prepareStatement(String sql, String[] columnNames)
            throws SQLException {
        return new TreasureDataPreparedStatement(client, database, sql); // TODO
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType,
        int resultSetConcurrency) throws SQLException {
        return new TreasureDataPreparedStatement(client, database, sql); // TODO
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        return new TreasureDataPreparedStatement(client, database, sql); // TODO
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#releaseSavepoint(java.sql.Savepoint)
     */
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#rollback()
     */
    public void rollback() throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#rollback(java.sql.Savepoint)
     */
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#setAutoCommit(boolean)
     */
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#setCatalog(java.lang.String)
     */
    public void setCatalog(String catalog) throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#setClientInfo(java.util.Properties)
     */
    public void setClientInfo(Properties properties)
            throws SQLClientInfoException {
        throw new SQLClientInfoException("Method not supported", null);
    }
    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#setClientInfo(java.lang.String, java.lang.String)
     */
    public void setClientInfo(String name, String value)
            throws SQLClientInfoException {
        throw new SQLClientInfoException("Method not supported", null);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#setHoldability(int)
     */
    public void setHoldability(int holdability) throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#setReadOnly(boolean)
     */
    public void setReadOnly(boolean readOnly) throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#setSavepoint()
     */
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#setSavepoint(java.lang.String)
     */
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLException("Method not supported");
    }
    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#setTransactionIsolation(int)
     */
    public void setTransactionIsolation(int level) throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#setTypeMap(java.util.Map)
     */
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLException("Method not supported");
    }


    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
     */
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Wrapper#unwrap(java.lang.Class)
     */
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("Method not supported");
    }
}
