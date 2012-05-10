package com.treasure_data.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.hsqldb.result.ResultConstants;

public class TDStatement extends TDStatementBase implements Statement {
    private int fetchSize = 50;

    public TDStatement(TDConnection conn) {
        super(conn);
    }

    public void addBatch(String sql) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public void cancel() throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public void clearBatch() throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public boolean execute(String sql) throws SQLException {
        // TODO: this should really check if there are results, but there's no easy
        // way to do that without calling rs.next();
        return executeQuery(sql) != null;
    }

    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return execute(sql);
    }

    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return execute(sql);
    }

    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return execute(sql);
    }

    public int[] executeBatch() throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public synchronized ResultSet executeQuery(String sql) throws SQLException {
        fetchResult(sql, ResultConstants.EXECDIRECT);
        return getResultSet();
    }

    public int executeUpdate(String sql) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return executeUpdate(sql);
    }

    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return executeUpdate(sql);
    }

    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return executeUpdate(sql);
    }

    public Connection getConnection() throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public int getFetchDirection() throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public int getFetchSize() throws SQLException {
        return fetchSize;
    }

    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public int getMaxFieldSize() throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public boolean getMoreResults() throws SQLException {
        return getMoreResults(CLOSE_CURRENT_RESULT);
    }

    public boolean getMoreResults(int current) throws SQLException {
        return true;
    }

    public int getQueryTimeout() throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public int getResultSetConcurrency() throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public int getResultSetHoldability() throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public int getResultSetType() throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public boolean isPoolable() throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public void setCursorName(String name) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public void setFetchDirection(int direction) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public void setFetchSize(int rows) throws SQLException {
        fetchSize = rows;
    }

    public void setMaxFieldSize(int max) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public void setMaxRows(int max) throws SQLException {
        if (max < 0) {
            throw new SQLException("max must be >= 0");
        }
        maxRows = max;
    }

    public void setPoolable(boolean poolable) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public void setQueryTimeout(int seconds) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }
}
