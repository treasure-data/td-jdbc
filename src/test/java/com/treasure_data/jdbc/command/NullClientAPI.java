package com.treasure_data.jdbc.command;

import java.util.List;
import java.util.Map;

import org.junit.Ignore;
import org.msgpack.unpacker.Unpacker;

import com.treasure_data.client.ClientException;
import com.treasure_data.jdbc.TDResultSetBase;
import com.treasure_data.jdbc.command.ClientAPI;
import com.treasure_data.model.DatabaseSummary;
import com.treasure_data.model.Job;
import com.treasure_data.model.JobSummary;
import com.treasure_data.model.TableSummary;

@Ignore
public class NullClientAPI implements ClientAPI {
    public NullClientAPI() {
    }

    public List<DatabaseSummary> showDatabases() throws ClientException {
        return null;
    }

    public List<TableSummary> showTables() throws ClientException {
        return null;
    }

    public boolean drop(String tableName) throws ClientException {
        return true;
    }

    public boolean create(String table) throws ClientException {
        return true;
    }

    public boolean insert(String tableName, Map<String, Object> record) throws ClientException {
        return true;
    }

    public boolean flush() {
        return true;
    }

    public TDResultSetBase select(String sql) throws ClientException {
        return null;
    }

    public TDResultSetBase select(String sql, int queryTimeout) throws ClientException {
        return null;
    }

    public JobSummary waitJobResult(Job job) throws ClientException {
        return null;
    }

    public Unpacker getJobResult(Job job) throws ClientException {
        return null;
    }

    public void close() throws ClientException {
    }
}
