package com.treasure_data.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;

import com.treasure_data.client.ClientException;
import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.model.Database;
import com.treasure_data.model.Job;
import com.treasure_data.model.SubmitJobRequest;
import com.treasure_data.model.SubmitJobResult;

public class TDPreparedStatement extends TDStatementBase
        implements PreparedStatement {
    private final String sql;

    /**
     * save the SQL parameters {paramLoc:paramValue}
     */
    private final HashMap<Integer, String> parameters = new HashMap<Integer, String>();

    private SQLWarning warnings = null;

    /**
     * keep the current ResultRet update count
     */
    private final int updateCount = 0;

    public TDPreparedStatement(TDConnection conn, String sql) {
        super(conn);
        this.sql = sql;
    }

    public void addBatch() throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public void clearParameters() throws SQLException {
        this.parameters.clear();
    }

    public boolean execute() throws SQLException {
        return executeImmediate(sql) != null;
    }

    public ResultSet executeQuery() throws SQLException {
        return executeImmediate(sql);
    }

    public int executeUpdate() throws SQLException {
        executeImmediate(sql);
        return updateCount;
    }

    private synchronized ResultSet executeImmediate(String sql)
            throws SQLException {
        Job job = new Job(database, sql);
        clearWarnings();
        currentResultSet = null;
        if (sql.contains("?")) {
            sql = updateSql(sql, parameters);
        }

        // submit a job
        // FIXME #MN
        try {
            SubmitJobRequest request = new SubmitJobRequest(job);
            SubmitJobResult result = client.submitJob(request);
            job = result.getJob();
        } catch (ClientException e) {
            throw new SQLException(e.toString(), "08S01");
        }

        currentResultSet = new TDQueryResultSet(client, maxRows, job);
        return currentResultSet;
    }

    /**
     * update the SQL string with parameters set by setXXX methods of
     * {@link PreparedStatement}
     * 
     * @param sql
     * @param parameters
     * @return updated SQL string
     */
    private String updateSql(final String sql, HashMap<Integer, String> parameters) {
        StringBuffer newSql = new StringBuffer(sql);

        int paramLoc = 1;
        while (getCharIndexFromSqlByParamLocation(sql, '?', paramLoc) > 0) {
            // check the user has set the needs parameters
            if (parameters.containsKey(paramLoc)) {
                int tt = getCharIndexFromSqlByParamLocation(newSql.toString(),
                        '?', 1);
                newSql.deleteCharAt(tt);
                newSql.insert(tt, parameters.get(paramLoc));
            }
            paramLoc++;
        }

        return newSql.toString();

    }

    /**
     * Get the index of given char from the SQL string by parameter location
     * </br> The -1 will be return, if nothing found
     * 
     * @param sql
     * @param cchar
     * @param paramLoc
     * @return
     */
    private int getCharIndexFromSqlByParamLocation(final String sql,
            final char cchar, final int paramLoc) {
        int signalCount = 0;
        int charIndex = -1;
        int num = 0;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '\'' || c == '\\') {
                // record the count of char "'" and char "\"
                signalCount++;
            } else if (c == cchar && signalCount % 2 == 0) {
                // check if the ? is really the parameter
                num++;
                if (num == paramLoc) {
                    charIndex = i;
                    break;
                }
            }
        }
        return charIndex;
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLException("Method not supported");
    }

    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setArray(int i, Array x) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setAsciiStream(int i, InputStream in) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setAsciiStream(int i, InputStream in, int length) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setAsciiStream(int i, InputStream in, long length) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setBigDecimal(int parameterIndex, BigDecimal x)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setBinaryStream(int i, InputStream x) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setBinaryStream(int i, InputStream in, int length) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setBinaryStream(int i, InputStream in, long length) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setBlob(int i, Blob x) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setBlob(int i, InputStream inputStream) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setBlob(int i, InputStream inputStream, long length) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        this.parameters.put(parameterIndex, "" + x);
    }

    public void setByte(int i, byte x) throws SQLException {
        this.parameters.put(i, "" + x);
    }

    public void setBytes(int i, byte[] x) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setCharacterStream(int i, Reader reader) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setCharacterStream(int i, Reader reader, int length) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setCharacterStream(int i, Reader reader, long length) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setClob(int i, Clob x) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setClob(int i, Reader reader) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setClob(int i, Reader reader, long length) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setDate(int i, Date x) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setDate(int i, Date x, Calendar cal) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setDouble(int i, double x) throws SQLException {
        parameters.put(i, "" + x);
    }

    public void setFloat(int i, float x) throws SQLException {
        parameters.put(i, "" + x);
    }

    public void setInt(int i, int x) throws SQLException {
        parameters.put(i, "" + x);
    }

    public void setLong(int i, long x) throws SQLException {
        parameters.put(i, "" + x);
    }

    public void setNCharacterStream(int i, Reader value) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setNCharacterStream(int i, Reader value, long length)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setNClob(int i, NClob value) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setNClob(int i, Reader reader) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setNClob(int i, Reader reader, long length)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setNString(int i, String value)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setNull(int i, int sqlType) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setNull(int i, int sqlType, String typeName) throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.PreparedStatement#setObject(int, java.lang.Object)
     */
    public void setObject(int i, Object x) throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.PreparedStatement#setObject(int, java.lang.Object, int)
     */
    public void setObject(int i, Object x, int targetSqlType) throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.PreparedStatement#setObject(int, java.lang.Object, int, int)
     */
    public void setObject(int i, Object x, int targetSqlType, int scale)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.PreparedStatement#setRef(int, java.sql.Ref)
     */
    public void setRef(int i, Ref x) throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.PreparedStatement#setRowId(int, java.sql.RowId)
     */
    public void setRowId(int i, RowId x) throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.PreparedStatement#setSQLXML(int, java.sql.SQLXML)
     */
    public void setSQLXML(int i, SQLXML xmlObject)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setShort(int i, short x) throws SQLException {
        this.parameters.put(i, "" + x);
    }

    public void setString(int i, String x) throws SQLException {
        x = x.replace("'", "\\'");
        this.parameters.put(i, "'" + x + "'");
    }

    public void setTime(int i, Time x) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setTime(int i, Time x, Calendar cal)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setTimestamp(int i, Timestamp x)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setTimestamp(int i, Timestamp x, Calendar cal)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setURL(int i, URL x) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setUnicodeStream(int i, InputStream x, int length)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void addBatch(String sql) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void cancel() throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void clearBatch() throws SQLException {
        throw new SQLException("Method not supported");
    }

    public boolean execute(String sql) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public boolean execute(String sql, int autoGeneratedKeys)
            throws SQLException {
        return execute(sql);
    }

    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return execute(sql);
    }

    public boolean execute(String sql, String[] columnNames)
            throws SQLException {
        return execute(sql);
    }

    public int[] executeBatch() throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#executeQuery(java.lang.String)
     */
    public ResultSet executeQuery(String sql) throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#executeUpdate(java.lang.String)
     */
    public int executeUpdate(String sql) throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#executeUpdate(java.lang.String, int)
     */
    public int executeUpdate(String sql, int autoGeneratedKeys)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#executeUpdate(java.lang.String, int[])
     */
    public int executeUpdate(String sql, int[] columnIndexes)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#executeUpdate(java.lang.String,
     * java.lang.String[])
     */
    public int executeUpdate(String sql, String[] columnNames)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getConnection()
     */
    public Connection getConnection() throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getFetchDirection()
     */
    public int getFetchDirection() throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getFetchSize()
     */
    public int getFetchSize() throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getGeneratedKeys()
     */
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getMaxFieldSize()
     */
    public int getMaxFieldSize() throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getMaxRows()
     */
    public int getMaxRows() throws SQLException {
        return this.maxRows;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getMoreResults()
     */
    public boolean getMoreResults() throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getMoreResults(int)
     */
    public boolean getMoreResults(int current) throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getQueryTimeout()
     */
    public int getQueryTimeout() throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getResultSet()
     */
    public ResultSet getResultSet() throws SQLException {
        return currentResultSet;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getResultSetConcurrency()
     */
    public int getResultSetConcurrency() throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getResultSetHoldability()
     */
    public int getResultSetHoldability() throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getResultSetType()
     */
    public int getResultSetType() throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getUpdateCount()
     */
    public int getUpdateCount() throws SQLException {
        return updateCount;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getWarnings()
     */
    public SQLWarning getWarnings() throws SQLException {
        return warnings;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#isClosed()
     */
    public boolean isClosed() throws SQLException {
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#isPoolable()
     */
    public boolean isPoolable() throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#setCursorName(java.lang.String)
     */
    public void setCursorName(String name) throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#setEscapeProcessing(boolean)
     */
    public void setEscapeProcessing(boolean enable) throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#setFetchDirection(int)
     */
    public void setFetchDirection(int direction) throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#setFetchSize(int)
     */
    public void setFetchSize(int rows) throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#setMaxFieldSize(int)
     */
    public void setMaxFieldSize(int max) throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#setMaxRows(int)
     */
    public void setMaxRows(int max) throws SQLException {
        if (max < 0) {
            throw new SQLException("max must be >= 0");
        }
        this.maxRows = max;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#setPoolable(boolean)
     */
    public void setPoolable(boolean poolable) throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#setQueryTimeout(int)
     */
    public void setQueryTimeout(int seconds) throws SQLException {
        // throw new SQLException("Method not supported");
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
