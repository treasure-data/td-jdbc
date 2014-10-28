package com.treasure_data.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import com.treasure_data.jdbc.command.CommandExecutor;
import com.treasure_data.jdbc.command.CommandContext;

public abstract class TDStatementBase implements Statement {

    protected TDConnection conn;

    protected CommandExecutor exec;

    protected int queryTimeout = -1; // seconds

    protected TDResultSetBase currentResultSet = null;

    protected int maxRows = 0;

    /**
     * add SQLWarnings to the warningCharn if need.
     */
    protected SQLWarning warningChain = null;

    /**
     * keep the current ResultRet update count
     */
    private int updateCount = 0;

    private boolean isEscapeProcessing;

    protected TDStatementBase(TDConnection conn) {
        this.conn = conn;
        exec = new CommandExecutor(this.conn.getClientAPI());
    }

    public Connection getConnection() throws SQLException {
        return conn;
    }

    public CommandExecutor getCommandExecutor() {
        return exec;
    }

    public boolean isClosed() throws SQLException {
//        if (currentResultSet != null) {
//            return false && currentResultSet.isClosed();
//        }
        return false;
    }

    public void close() throws SQLException {
        if (currentResultSet != null) {
            currentResultSet.close();
        }
    }

    public TDResultSetBase getResultSet() throws SQLException {
        if (currentResultSet != null) {
            currentResultSet.setStatement(this);
        }
        return currentResultSet;
    }

    public int getUpdateCount() throws SQLException {
        return updateCount;
    }

    public void setEscapeProcessing(boolean enable) throws SQLException {
        isEscapeProcessing = enable;
    }

    public SQLWarning getWarnings() throws SQLException {
        return warningChain;
    }

    public void clearWarnings() throws SQLException {
        warningChain = null;
    }

    public int getQueryTimeout() throws SQLException {
        return this.queryTimeout;
    }

    /*
     * Sets the number of seconds the driver will wait for a Statement object to
     * execute to the given number of seconds. If the limit is exceeded, an
     * SQLException is thrown. A JDBC driver must apply this limit to the
     * execute, executeQuery and executeUpdate methods. JDBC driver
     * implementations may also apply this limit to ResultSet methods (consult
     * your driver vendor documentation for details).
     *
     * @param seconds - the new query timeout limit in seconds; zero means there
     * is no limit
     *
     * @throws SQLException - if a database access error occurs, this method is
     * called on a closed Statement or the condition seconds >= 0 is not
     * satisfied
     *
     * @see java.sql.Statement#setQueryTimeout(int)
     */
    public void setQueryTimeout(int seconds) throws SQLException {
        if (seconds < 0) {
            throw new SQLException("seconds must be >= 0");
        }
        this.queryTimeout = seconds;
    }

    public int getMaxRows() throws SQLException {
        return maxRows;
    }

    public void setMaxRows(int max) throws SQLException {
        if (max < 0) {
            throw new SQLException("max must be >= 0");
        }
        maxRows = max;
    }

    protected CommandContext fetchResult(String sql)
            throws SQLException {
        CommandContext context = createCommandContext(sql);
        fetchResult(context);
        return context;
    }

    protected void fetchResult(CommandContext context)
            throws SQLException {
        try {
            exec.execute(context);
            currentResultSet = context.resultSet;
        } catch (Throwable t) {
            if (t instanceof SQLException) {
                throw (SQLException) t;
            } else {
                throw new SQLException(t);
            }
        }
    }

    protected CommandContext createCommandContext(String sql) {
        CommandContext context = new CommandContext();
        context.sql = sql;
        context.queryTimeout = queryTimeout;
        return context;
    }
}
