/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.treasuredata.jdbc;

import com.treasure_data.client.ClientException;
import com.treasuredata.jdbc.command.ClientAPI;
import com.treasure_data.model.Job;
import com.treasure_data.model.JobSummary;
import org.json.simple.JSONValue;
import org.msgpack.type.ArrayValue;
import org.msgpack.type.Value;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TDResultSet
        extends TDResultSetBase
{
    private static Logger LOG = Logger.getLogger(TDResultSet.class.getName());

    private ExecutorService executor = Executors.newSingleThreadExecutor();

    private ClientAPI clientApi;

    private int maxRows = 0;

    private int rowsFetched = 0;

    private int fetchSize = 50;

    private int queryTimeout = 0; // seconds

    private ClientAPI.ExtUnpacker fetchedRows;

    private Iterator<Value> fetchedRowsItr;

    private Job job;

    public TDResultSet(ClientAPI clientApi, int maxRows, Job job)
    {
        this(clientApi, maxRows, job, 0);
    }

    public TDResultSet(ClientAPI clientApi, int maxRows, Job job, int queryTimeout)
    {
        this.clientApi = clientApi;
        this.maxRows = maxRows;
        this.job = job;
        this.queryTimeout = queryTimeout;
    }

    @Override
    public void setFetchSize(int rows)
            throws SQLException
    {
        fetchSize = rows;
    }

    @Override
    public int getFetchSize()
            throws SQLException
    {
        return fetchSize;
    }

    public void setMaxRows(int maxRows)
    {
        this.maxRows = maxRows;
    }

    public int getMaxRows()
    {
        return maxRows;
    }

    @Override
    public void close()
            throws SQLException
    {
        if (fetchedRows != null) {
            try {
                fetchedRows.getUnpacker().close();
                LOG.info("closed file based unpacker");
            }
            catch (IOException e) {
                throw new SQLException(e);
            }

            File f = fetchedRows.getFile();
            if (f != null) {
                // temp file is deleted
                String fname = f.getAbsolutePath();
                f.delete();
                LOG.info("deleted temp file: " + fname);
            }
        }

        // TODO #MN should check that this method is really called
        if (executor != null) {
            try {
                executor.shutdownNow();
            }
            catch (Throwable t) {
                throw new SQLException(t);
            }
            finally {
                executor = null;
            }
        }
    }

    /**
     * Moves the cursor down one row from its current position.
     *
     * @throws SQLException if a database access error occurs.
     * @see java.sql.ResultSet#next()
     */
    public boolean next()
            throws SQLException
    {
        try {
            if (fetchedRows == null) {
                fetchedRows = fetchRows();
                fetchedRowsItr = fetchedRows.getUnpacker().iterator();
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
                LOG.fine(String.format("fetched row(%d): %s", rowsFetched, row));
            }
        }
        catch (Exception e) {
            if (e instanceof SQLException) {
                throw (SQLException) e;
            }
            else {
                throw new SQLException("Error retrieving next row", e);
            }
        }
        // NOTE: fetchOne dosn't throw new SQLException("Method not supported").
        return true;
    }

    @Override
    public ResultSetMetaData getMetaData()
            throws SQLException
    {
        try {
            JobSummary jobSummary = clientApi.waitJobResult(job);
            initColumnNamesAndTypes(jobSummary.getResultSchema());
            return new TDResultSetMetaData(job.getJobID(), columnNames, columnTypes);
        }
        catch (ClientException e) {
            throw new SQLException(e);
        }
    }

    private ClientAPI.ExtUnpacker fetchRows()
            throws SQLException
    {
        JobSummary jobSummary = null;

        Callable<JobSummary> callback = new Callable<JobSummary>()
        {
            @Override
            public JobSummary call()
                    throws Exception
            {
                JobSummary jobSummary = clientApi.waitJobResult(job);
                return jobSummary;
            }
        };

        Future<JobSummary> future = executor.submit(callback);
        try {
            if (queryTimeout <= 0) {
                jobSummary = future.get();
            }
            else {
                jobSummary = future.get(queryTimeout, TimeUnit.SECONDS);
            }
        }
        catch (InterruptedException e) {
            // ignore
        }
        catch (TimeoutException e) {
            throw new SQLException(e);
        }
        catch (ExecutionException e) {
            throw new SQLException(e.getCause());
        }

        if (jobSummary == null) {
            throw new SQLException("job result is null");
        }

        try {
            initColumnNamesAndTypes(jobSummary.getResultSchema());
            return clientApi.getJobResult2(job);
        }
        catch (ClientException e) {
            throw new SQLException(e);
        }
    }

    private void initColumnNamesAndTypes(String resultSchema)
    {
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine("resultSchema: " + resultSchema);
        }

        if (resultSchema == null) {
            LOG.warning("Illegal resultSchema: null");
            return;
        }

        @SuppressWarnings("unchecked")
        List<List<String>> cols = (List<List<String>>) JSONValue.parse(resultSchema);
        if (cols == null) {
            LOG.warning("Illegal resultSchema: " + resultSchema);
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
