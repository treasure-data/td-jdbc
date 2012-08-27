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

import com.treasure_data.jdbc.command.ClientAPI;
import com.treasure_data.jdbc.command.TDClientAPI;
import com.treasure_data.model.Database;

public class TDConnection implements Connection, Constants {

    private boolean autoCommit = false;

    private boolean readOnly = false;

    private ClientAPI api;

    private Properties props;

    private Database database;

    private int maxRows = 50;

    private SQLWarning warnings = null;

    public TDConnection(String uri, Properties props) throws SQLException {
        if (uri == null || uri.isEmpty() || !uri.startsWith(URI_PREFIX)) {
            throw new SQLException("Invalid URI: " + uri);
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

        // set host and port properties to props
        props.setProperty(Config.TD_API_SERVER_HOST, host);
        props.setProperty(Config.TD_API_SERVER_PORT, "" + port);
        this.props = props;

        // create a Database object
        if (fragments.length > 1) {
            database = new Database(fragments[1]);
        } else {
            throw new SQLException(
            "Cannot create a connection because database is not specified");
        }

        // create a ClientAPI object
        this.api = new TDClientAPI(this);
    }

    public ClientAPI getClientAPI() {
        return api;
    }

    public Properties getProperties() {
        return props;
    }

    public Database getDatabase() {
        return database;
    }

    public int getMaxRows() {
        return maxRows;
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
        // ignore
    }

    public boolean isClosed() throws SQLException {
        return false;
    }

    public void commit() throws SQLException {
        // ignore TODO #MN consider more
    }

    public Array createArrayOf(String typeName, Object[] elements)
            throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public Blob createBlob() throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public Clob createClob() throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public NClob createNClob() throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public SQLXML createSQLXML() throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
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
        return new TDStatement(this);
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return createStatement();
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        return createStatement();
    }

    public Struct createStruct(String typeName, Object[] attributes)
            throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public boolean getAutoCommit() throws SQLException {
        return autoCommit;
    }

    public String getCatalog() throws SQLException {
        return "";
    }

    public Properties getClientInfo() throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public String getClientInfo(String name) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public int getHoldability() throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        return new TDDatabaseMetaData(getClientAPI());
    }

    public int getTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public boolean isReadOnly() throws SQLException {
        return readOnly;
    }

    public boolean isValid(int timeout) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public String nativeSQL(String sql) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public CallableStatement prepareCall(String sql)
            throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public CallableStatement prepareCall(String sql,
            int resultSetType, int resultSetConcurrency)
            throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public CallableStatement prepareCall(String sql,
            int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public PreparedStatement prepareStatement(String sql)
            throws SQLException {
        return new TDPreparedStatement(this, sql);
    }

    public PreparedStatement prepareStatement(String sql,
            int autoGeneratedKeys) throws SQLException {
        return prepareStatement(sql);
    }

    public PreparedStatement prepareStatement(String sql,
            int[] columnIndexes) throws SQLException {
        return prepareStatement(sql);
    }

    public PreparedStatement prepareStatement(String sql,
            String[] columnNames) throws SQLException {
        return prepareStatement(sql);
    }

    public PreparedStatement prepareStatement(String sql,
            int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return prepareStatement(sql);
    }

    public PreparedStatement prepareStatement(String sql,
            int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        return prepareStatement(sql);
    }

    public void releaseSavepoint(Savepoint savepoint)
            throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public void rollback() throws SQLException {
        // ignore TODO #MN consider more
    }

    public void rollback(Savepoint savepoint) throws SQLException {
        // ignore TODO #MN consider more
    }

    public void setAutoCommit(boolean autoCommit)
            throws SQLException {
        this.autoCommit = autoCommit;
    }

    public void setCatalog(String catalog) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public void setClientInfo(Properties properties)
            throws SQLClientInfoException {
        throw new SQLClientInfoException("Method not supported", null);
    }

    public void setClientInfo(String name, String value)
            throws SQLClientInfoException {
        throw new SQLClientInfoException("Method not supported", null);
    }

    public void setHoldability(int holdability) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        this.readOnly = readOnly;
    }

    public Savepoint setSavepoint() throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public void setTransactionIsolation(int level) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }
}
