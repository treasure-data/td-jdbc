package com.treasure_data.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import com.treasure_data.jdbc.command.CommandExecutor;
import com.treasure_data.jdbc.command.TreasureDataClientAdaptor;

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

}
