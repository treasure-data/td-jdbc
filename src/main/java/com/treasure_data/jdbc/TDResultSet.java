package com.treasure_data.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.simple.JSONValue;
import org.msgpack.type.ArrayValue;
import org.msgpack.type.Value;
import org.msgpack.unpacker.Unpacker;

import com.treasure_data.client.ClientException;
import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.jdbc.command.ClientAPI;
import com.treasure_data.model.GetJobResultRequest;
import com.treasure_data.model.GetJobResultResult;
import com.treasure_data.model.Job;
import com.treasure_data.model.JobResult;
import com.treasure_data.model.JobSummary;
import com.treasure_data.model.ShowJobRequest;
import com.treasure_data.model.ShowJobResult;

public class TDResultSet extends TDResultSetBase {
    private static Logger LOG = Logger.getLogger(TDResultSet.class.getName());

    private ClientAPI clientApi;

    private int maxRows = 0;

    private int rowsFetched = 0;

    private int fetchSize = 50;

    private Unpacker fetchedRows;

    private Iterator<Value> fetchedRowsItr;

    private Job job;

    public TDResultSet(ClientAPI clientApi, int maxRows, Job job) {
        this.clientApi = clientApi;
        this.maxRows = maxRows;
        this.job = job;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        return fetchSize;
    }

    @Override
    public void close() throws SQLException {
        clientApi = null;
        fetchedRows = null;
        fetchedRowsItr = null;
    }

    /**
     * Moves the cursor down one row from its current position.
     *
     * @see java.sql.ResultSet#next()
     * @throws SQLException     if a database access error occurs.
     */
    public boolean next() throws SQLException {
        if (maxRows > 0 && rowsFetched >= maxRows) {
            return false;
        }

        try {
            if (fetchedRows == null) {
                fetchedRows = fetchRows(fetchSize);
                fetchedRowsItr = fetchedRows.iterator();
            }

            if (!fetchedRowsItr.hasNext()) {
                return false;
            }

            ArrayValue vs = (ArrayValue) fetchedRowsItr.next();
            row = new ArrayList<Object>(vs.size());
            for (int i = 0; i < vs.size(); i++) {
                row.add(i, vs.get(i));
            }
            rowsFetched++;

            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("Fetched row: " + row);
            }
        } catch (Exception e) {
            throw new SQLException("Error retrieving next row", e);
        }
        // NOTE: fetchOne dosn't throw new SQLException("Method not supported").
        return true;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        try {
            JobSummary jobSummary = clientApi.waitJobResult(job);
            initColumnNamesAndTypes(jobSummary);
            return super.getMetaData();
        } catch (ClientException e) {
            throw new SQLException(e);
        }
    }

    private Unpacker fetchRows(int fetchSize) throws SQLException {
        try {
            JobSummary jobSummary = clientApi.waitJobResult(job);
            initColumnNamesAndTypes(jobSummary);
            return clientApi.getJobResult(job);
        } catch (ClientException e) {
            throw new SQLException(e);
        }
    }

    private void initColumnNamesAndTypes(JobSummary job) {
        String resultSchema = job.getResultSchema();
        @SuppressWarnings("unchecked")
        List<List<String>> cols = (List<List<String>>) JSONValue.parse(resultSchema);
        if (cols == null) {
            return;
        }
        columnNames = new ArrayList<String>(cols.size());
        columnTypes = new ArrayList<String>(cols.size());
        for (List<String> col : cols) {
            columnNames.add(col.get(0));
            columnTypes.add(col.get(1));
        }
    }

}
