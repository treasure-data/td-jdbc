package com.treasure_data.jdbc.command;

import java.util.List;
import java.util.Map;

import org.msgpack.unpacker.Unpacker;

import com.treasure_data.client.ClientException;
import com.treasure_data.jdbc.TDResultSetBase;
import com.treasure_data.jdbc.command.ClientAPI;
import com.treasure_data.jdbc.command.ClientAPI.ExtUnpacker;
import com.treasure_data.model.DatabaseSummary;
import com.treasure_data.model.Job;
import com.treasure_data.model.JobSummary;
import com.treasure_data.model.TableSummary;

public class NullClientAPI implements ClientAPI {
    public NullClientAPI() {
    }

    public List<DatabaseSummary> showDatabases() throws ClientException {
        return null;
    }

    public DatabaseSummary showDatabase() throws ClientException {
        return null;
    }

    public List<TableSummary> showTables() throws ClientException {
        return null;
    }

    public boolean flush() {
        return true;
    }

    public TDResultSetBase select(String sql) throws ClientException {
        return null;
    }

    public TDResultSetBase select(String sql, int queryTimeout)
            throws ClientException {
        return null;
    }

    public JobSummary waitJobResult(Job job) throws ClientException {
        return null;
    }

    public Unpacker getJobResult(Job job) throws ClientException {
        return null;
    }

    public ExtUnpacker getJobResult2(Job job) throws ClientException {
        return new ExtUnpacker(null, getJobResult(job));
    }

    public void close() throws ClientException {
    }
}
