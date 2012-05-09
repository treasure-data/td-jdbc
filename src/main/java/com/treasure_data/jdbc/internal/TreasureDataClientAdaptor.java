package com.treasure_data.jdbc.internal;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import com.treasure_data.client.ClientException;
import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.jdbc.TreasureDataQueryResultSet;
import com.treasure_data.logger.TreasureDataLogger;
import com.treasure_data.model.Database;
import com.treasure_data.model.Job;
import com.treasure_data.model.SubmitJobRequest;
import com.treasure_data.model.SubmitJobResult;

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
        return logger.log(tableName, record);
    }

    public boolean flush() { // TODO for debug
        TreasureDataLogger logger = TreasureDataLogger.getLogger(database.getName());
        TreasureDataLogger.close();
        return true;
    }

    public Job select(String sql) {
        try {
            Job job = new Job(database, sql);

            // submit a job
            SubmitJobRequest request = new SubmitJobRequest(job);
            SubmitJobResult result = client.submitJob(request);
            return result.getJob();
        } catch (ClientException e) {
            // TODO what exception should we throw?
            throw new RuntimeException(e);
        }
    }
}
