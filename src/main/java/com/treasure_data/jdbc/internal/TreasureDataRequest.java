package com.treasure_data.jdbc.internal;

import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.jdbc.Constants;

public class TreasureDataRequest {

    private TreasureDataClient client;

    private int mode = Constants.RESULT_EXECDIRECT; // TODO

    private String mainString;

    private long maxRows;

    public TreasureDataRequest(TreasureDataClient client) {
        this.client = client;
    }

    public int getMode() {
        return mode;
    }

    public void setMainString(String sql) {
        this.mainString = sql;
    }

    public String getMainString() {
        return mainString;
    }

    public void setMaxRows(long maxRows) {
        this.maxRows = maxRows;
    }

    public TreasureDataResult execute() {
        // TODO
        // use client
        throw new RuntimeException();
    }

}
