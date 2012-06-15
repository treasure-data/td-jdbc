package com.treasure_data.jdbc.command;

import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

import org.msgpack.unpacker.Unpacker;

import com.treasure_data.client.ClientException;
import com.treasure_data.jdbc.TDResultSetBase;
import com.treasure_data.model.Job;
import com.treasure_data.model.JobSummary;
import com.treasure_data.model.TableSummary;

public class NullClientAPI implements ClientAPI {
    public NullClientAPI() {
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

    public JobSummary waitJobResult(Job job) throws ClientException {
        return null;
    }

    public Unpacker getJobResult(Job job) throws ClientException {
        return null;
    }
}
