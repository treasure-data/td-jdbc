package com.treasure_data.jdbc.command;

import java.sql.ResultSet;
import java.util.Map;

import com.treasure_data.client.ClientException;
import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.jdbc.TDConnection;
import com.treasure_data.jdbc.TDQueryResultSet;
import com.treasure_data.logger.TreasureDataLogger;
import com.treasure_data.model.Database;
import com.treasure_data.model.Job;
import com.treasure_data.model.SubmitJobRequest;
import com.treasure_data.model.SubmitJobResult;

public class TDClientAPI implements ClientAPI {
    private TreasureDataClient client;

    private Database database;

    public TDClientAPI(TDConnection conn) {
        this(new TreasureDataClient(conn.getProperties()), conn.getDatabase());
    }

    public TDClientAPI(TreasureDataClient client, Database database) {
        this.client = client;
        this.database = database;
    }

    public TreasureDataClient getTreasureDataClient() {
        return client;
    }

    public Database getDatabase() {
        return database;
    }

    public boolean create(String table) {
        try {
            client.createTable(database, table);
            return true;
        } catch (ClientException e) {
            return false;
        }
    }

    public boolean insert(String tableName, Map<String, Object> record) {
        TreasureDataLogger logger = TreasureDataLogger.getLogger(database.getName());
        return logger.log(tableName, record);
    }

    public boolean flush() {
        TreasureDataLogger logger = TreasureDataLogger.getLogger(database.getName());
        logger.flush();
        return true;
    }

    public ResultSet select(String sql) throws ClientException {
        ResultSet rs = null;

        Job job = new Job(database, sql);
        SubmitJobRequest request = new SubmitJobRequest(job);
        SubmitJobResult result = client.submitJob(request);
        job = result.getJob();

        if (job != null) {
            rs = new TDQueryResultSet(client, 50, job);
        }
        return rs;
    }
}
