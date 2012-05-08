package com.treasure_data.jdbc.internal;

import org.hsqldb.result.ResultConstants;

public class Result {

    private String mainString;
    private int updateCount;
    private int fetchSize;
    private int statementReturnType;
    private int queryTimeout;
    private int rsProperties;
    private int generateKeys;
    public ResultMetaData metaData;
    private ResultMetaData generatedMetaData;

    public void setPrepareOrExecuteProperties(String sql, int maxRows,
            int fetchSize, int statementReturnType, int timeout,
            int resultSetProperties, int keyMode, int[] generatedIndexes,
            String[] generatedNames) {

        mainString               = sql;
        updateCount              = maxRows;
        this.fetchSize           = fetchSize;
        this.statementReturnType = statementReturnType;
        this.queryTimeout        = timeout;
        rsProperties             = resultSetProperties;
        generateKeys             = keyMode;
        generatedMetaData = ResultMetaData.newGeneratedColumnsMetaData(
                generatedIndexes, generatedNames);
    }

    public byte mode;

    public Result(int mode) {
        this.mode = (byte) mode;
    }

    public int getType() {
        return mode;
    }

    public int getStatementType() {
        return statementReturnType;
    }

    public boolean isData() {
        return mode == ResultConstants.DATA
               || mode == ResultConstants.DATAHEAD;
    }

    public boolean isError() {
        return mode == ResultConstants.ERROR;
    }

    public boolean isWarning() {
        return mode == ResultConstants.WARNING;
    }

    public boolean isUpdateCount() {
        return mode == ResultConstants.UPDATECOUNT;
    }

    public boolean isSimpleValue() {
        return mode == ResultConstants.VALUE;
    }



}
