package com.treasure_data.jdbc.internal;

import java.util.Map;

import com.treasure_data.client.TreasureDataClient;

public class TreasureDataClientAdaptor implements ClientAdaptor {

    private TreasureDataClient client;

    public TreasureDataClientAdaptor(TreasureDataClient client) {
        this.client = client;
    }

    public boolean createTable(String table) {
        return true; // TODO
    }

    public boolean insertData(Map<String, Object> record) {
        return true; // TODO
    }

    public boolean select(String sql) {
        return true; // TODO
    }
}
