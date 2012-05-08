package com.treasure_data.jdbc;

import java.sql.SQLException;
import java.sql.SQLWarning;

import org.hsqldb.jdbc.JDBCResultSet;
import org.hsqldb.jdbc.Util;
import org.hsqldb.result.ResultConstants;

import com.treasure_data.jdbc.internal.Result;

public abstract class JDBCAbstractStatement {
    public static final int CLOSE_CURRENT_RESULT  = 1;

    public static final int KEEP_CURRENT_RESULT   = 2;

    public static final int CLOSE_ALL_RESULTS     = 3;

    public static final int SUCCESS_NO_INFO       = -2;

    public static final int EXECUTE_FAILED        = -3;

    public static final int RETURN_GENERATED_KEYS = 1;

    public static final int NO_GENERATED_KEYS     = 2;

    protected boolean isEscapeProcessing = true;

    protected Result resultOut;

    protected Result resultIn;

    protected int queryTimeout;

    protected int rsProperties;

    protected Result errorResult;

    protected Result generatedResult;

    protected JDBCResultSet generatedResultSet;

    protected SQLWarning sqlWarning;

    void performPostExecute() throws SQLException {
        // TODO
    }

    boolean getMoreResults(int current) throws SQLException {
        return true; // TODO
    }

}
