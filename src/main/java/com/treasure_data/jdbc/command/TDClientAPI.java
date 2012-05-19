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

    private int maxRows = 50;

    public TDClientAPI(TDConnection conn) {
        this(new TreasureDataClient(conn.getProperties()),
                conn.getDatabase(), conn.getMaxRows());
    }

    public TDClientAPI(TreasureDataClient client, Database database) {
        this(client, database, 50);
    }

    public TDClientAPI(TreasureDataClient client, Database database, int maxRows) {
        this.client = client;
        this.database = database;
        this.maxRows = maxRows;
    }

    public boolean drop(String table) throws ClientException {
        client.deleteTable(database.getName(), table);
        return true;
    }

    public boolean create(String table) throws ClientException {
        client.createTable(database, table);
        return true;
    }

    public boolean insert(String tableName, Map<String, Object> record)
            throws ClientException {
        TreasureDataLogger logger = TreasureDataLogger.getLogger(database.getName());
        return logger.log(tableName, record);
    }

    public ResultSet select(String sql) throws ClientException {
        ResultSet rs = null;

        Job job = new Job(database, sql);
        SubmitJobRequest request = new SubmitJobRequest(job);
        SubmitJobResult result = client.submitJob(request);
        job = result.getJob();

        if (job != null) {
            rs = new TDQueryResultSet(client, maxRows, job);
        }
        return rs;
    }

    public boolean flush() {
        TreasureDataLogger logger = TreasureDataLogger.getLogger(database.getName());
        logger.flush();
        return true;
    }

}
