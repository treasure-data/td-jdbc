package com.treasure_data.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import com.treasure_data.jdbc.command.CommandExecutor;
import com.treasure_data.jdbc.command.TDClientAPI;
import com.treasure_data.jdbc.command.CommandContext;

public abstract class TDStatementBase implements Statement {

    protected CommandExecutor exec;

    protected ResultSet currentResultSet = null;

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
        exec = new CommandExecutor(new TDClientAPI(conn));
    }

    public void close() throws SQLException {
        currentResultSet = null;
    }

    public ResultSet getResultSet() throws SQLException {
        ResultSet tmp = currentResultSet;
        currentResultSet = null;
        return tmp;
    }

    public int getUpdateCount() throws SQLException {
        return updateCount;
    }

    public void setEscapeProcessing(boolean enable) throws SQLException {
        isEscapeProcessing = enable;
    }

    public boolean isClosed() throws SQLException {
        return true; // ignore
    }

    public SQLWarning getWarnings() throws SQLException {
        return warningChain;
    }

    public void clearWarnings() throws SQLException {
        warningChain = null;
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

    protected CommandContext fetchResult(String sql, int mode)
            throws SQLException {
        CommandContext context = new CommandContext();
        context.mode = mode;
        context.sql = sql;
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
}
