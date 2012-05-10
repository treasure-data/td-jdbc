package com.treasure_data.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import com.treasure_data.jdbc.command.CommandExecutor;
import com.treasure_data.jdbc.command.TreasureDataClientAdaptor;
import com.treasure_data.jdbc.command.Wrapper;

public abstract class TDStatementBase implements Statement {

    protected CommandExecutor exec;

    protected ResultSet currentResultSet = null;

    protected int maxRows = 0;

    /**
     * add SQLWarnings to the warningCharn if need.
     */
    protected SQLWarning warningChain = null;

    protected TDStatementBase(TDConnection conn) {
        exec = new CommandExecutor(new TreasureDataClientAdaptor(conn));
    }

    public void close() throws SQLException {
        currentResultSet = null;
    }

    public ResultSet getResultSet() throws SQLException {
        ResultSet tmp = currentResultSet;
        currentResultSet = null;
        return tmp;
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


    protected Wrapper fetchResult(String sql, int mode) throws SQLException {
        Wrapper w = new Wrapper();
        w.mode = mode;
        w.sql = sql;
        return fetchResult(w);
    }

    protected Wrapper fetchResult(Wrapper w) throws SQLException {
        try {
            exec.execute(w);
            currentResultSet = w.resultSet;
            return w;
        } catch (Throwable t) {
            if (t instanceof SQLException) {
                throw (SQLException) t;
            } else {
                throw new SQLException(t);
            }
        }
    }
}
