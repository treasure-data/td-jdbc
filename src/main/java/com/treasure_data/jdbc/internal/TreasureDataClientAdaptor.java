package com.treasure_data.jdbc.internal;

import java.io.IOException;
import java.util.Map;

import com.treasure_data.client.ClientException;
import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.logger.TreasureDataLogger;
import com.treasure_data.model.Database;

public class TreasureDataClientAdaptor implements ClientAdaptor {

    private TreasureDataClient client;

    private Database database;

    public TreasureDataClientAdaptor(TreasureDataClient client, Database database) {
        this.client = client;
        this.database = database;
    }

    public boolean createTable(String table) {
        try {
            client.createTable(database, table);
            return true;
        } catch (ClientException e) {
            return false;
        }
    }

    public boolean insertData(String tableName, Map<String, Object> record) {
        TreasureDataLogger logger = TreasureDataLogger.getLogger(database.getName());
        boolean ret = logger.log(tableName, record);
        System.out.println("ret: " + ret);
        return ret;
    }

    public boolean flush() {
        TreasureDataLogger logger = TreasureDataLogger.getLogger(database.getName());
        TreasureDataLogger.close();
        return true;
    }

    public boolean select(String sql) {
        return true; // TODO
    }
}
