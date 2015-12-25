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
package com.treasuredata.jdbc.command;

import com.treasure_data.client.ClientException;
import com.treasure_data.client.TreasureDataClient;
import com.treasuredata.jdbc.Config;
import com.treasuredata.jdbc.TDConnection;
import com.treasuredata.jdbc.TDResultSet;
import com.treasuredata.jdbc.TDResultSetBase;
import com.treasuredata.jdbc.TDResultSetMetaData;
import com.treasure_data.model.AuthenticateRequest;
import com.treasure_data.model.Database;
import com.treasure_data.model.DatabaseSummary;
import com.treasure_data.model.GetJobResultRequest;
import com.treasure_data.model.GetJobResultResult;
import com.treasure_data.model.Job;
import com.treasure_data.model.JobResult;
import com.treasure_data.model.JobResult2;
import com.treasure_data.model.JobSummary;
import com.treasure_data.model.SubmitJobRequest;
import com.treasure_data.model.SubmitJobResult;
import com.treasure_data.model.TableSummary;
import org.msgpack.MessagePack;
import org.msgpack.unpacker.Unpacker;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

public class TDClientAPI
        implements ClientAPI
{
    private static final Logger LOG = Logger.getLogger(TDClientAPI.class
            .getName());

    private final TreasureDataClient client;
    private final Config config;

    private final Database database;

    private int maxRows = 5000;

    public TDClientAPI(TDConnection conn)
            throws SQLException
    {
        this(conn.getConfig(), new TreasureDataClient(conn.getConfig().toProperties()), conn.getDatabase(), conn.getMaxRows());
    }

    TDClientAPI(Config config, TreasureDataClient client, Database database)
        throws SQLException
    {
        this(config, client, database, 5000);
    }

    TDClientAPI(Config config, TreasureDataClient client, Database database, int maxRows)
            throws SQLException
    {
        this.config = config;
        this.client = client;
        this.database = database;
        this.maxRows = maxRows;
        try {
            // Enable proxy configuration
            config.apply();
            checkCredentials();
        }
        catch (ClientException e) {
            throw new SQLException(e);
        }
    }

    private void checkCredentials()
            throws ClientException
    {
        String apiKey = client.getTreasureDataCredentials().getAPIKey();
        if (apiKey != null) {
            return;
        }

        client.authenticate(new AuthenticateRequest(config.user, config.password));
    }

    public List<DatabaseSummary> showDatabases()
            throws ClientException
    {
        return client.listDatabases();
    }

    public DatabaseSummary showDatabase()
            throws ClientException
    {
        List<DatabaseSummary> databases = client.listDatabases();
        for (DatabaseSummary db : databases) {
            if (db.getName().equals(database.getName())) {
                return db;
            }
        }
        return null;
    }

    public List<TableSummary> showTables()
            throws ClientException
    {
        return client.listTables(database);
    }

    public boolean drop(String table)
            throws ClientException
    {
        client.deleteTable(database.getName(), table);
        return true;
    }

    public boolean create(String table)
            throws ClientException
    {
        client.createTable(database, table);
        return true;
    }

    public TDResultSetBase select(String sql)
            throws ClientException
    {
        return select(sql, 0);
    }

    public TDResultSetBase select(String sql, int queryTimeout)
            throws ClientException
    {
        TDResultSetBase rs = null;

        Job job = new Job(database, config.type, sql, null);
        SubmitJobRequest request = new SubmitJobRequest(job);
        SubmitJobResult result = client.submitJob(request);
        job = result.getJob();

        if (job != null) {
            rs = new TDResultSet(this, maxRows, job, queryTimeout);
        }
        return rs;
    }

    public TDResultSetMetaData getMetaDataWithSelect1()
    {
        //  handle Presto/Hive differences
        //  https://console.treasuredata.com/jobs/24746696 Hive
        //  https://console.treasuredata.com/jobs/24745829 Presto
        List<String> names, types;
        switch(config.type) {
            case HIVE:
                names = Arrays.asList("_c0");
                types = Arrays.asList("int");
                break;
            case PRESTO:
                names = Arrays.asList("_col0");
                types = Arrays.asList("bigint");
                break;
            default:
                // pig, etc.
                throw new UnsupportedOperationException("Unsupported job type: " + config.type);
        }
        return new TDResultSetMetaData(new ArrayList<String>(names), new ArrayList<String>(types));
    }

    public JobSummary waitJobResult(Job job)
            throws ClientException
    {
        String jobID = job.getJobID();

        while (true) {
            JobSummary.Status stat = client.showJobStatus(job);

            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("Job status: " + stat);
            }

            if (stat == JobSummary.Status.SUCCESS) {
                LOG.fine("Job worked successfully.");
                break;
            }
            else if (stat == JobSummary.Status.ERROR) {
                JobSummary js = client.showJob(job);
                String msg = String.format(
                        "Job '%s' failed: got Job status 'error'", jobID);
                LOG.severe(msg);
                if (js.getDebug() != null) {
                    msg = msg + "\n" + js.getDebug().getStderr();
                    LOG.severe("cmdout:");
                    LOG.severe(js.getDebug().getCmdout());
                    LOG.severe("stderr:");
                    LOG.severe(js.getDebug().getStderr());
                }
                throw new ClientException(msg);
            }
            else if (stat == JobSummary.Status.KILLED) {
                String msg = String.format(
                        "Job '%s' failed: got Job status 'killed'", jobID);
                LOG.severe(msg);
                throw new ClientException(msg);
            }

            try {
                Thread.sleep(2 * 1000);
            }
            catch (InterruptedException e) {
                return null;
            }
        }

        return client.showJob(job);
    }

    public Unpacker getJobResult(Job job)
            throws ClientException
    {
        GetJobResultRequest request = new GetJobResultRequest(
                new JobResult(job));
        GetJobResultResult result = client.getJobResult(request);
        return result.getJobResult().getResult();
    }

    public ExtUnpacker getJobResult2(Job job)
            throws ClientException
    {
        File file = null;

        int retryCount = 0;
        int retryCountThreshold = config.resultRetryCountThreshold;
        while (true) {
            try {
                LOG.info("write the result to file");
                file = writeJobResultToTempFile(job);
                break;
            }
            catch (Throwable t) {
                LOG.warning("cought exception: message = " + t.getMessage());
                t.printStackTrace();

                // catch ClientException, IOException
                if (!(retryCount < retryCountThreshold)) {
                    throw new ClientException(
                            "re-try out writing: threashold = "
                                    + retryCountThreshold);
                }

                retryCount++;
                LOG.info("re-try writing: imcremented retryCount = "
                        + retryCount);
                long retryWaitTime = config.resultRetryWaitTimeMs;
                try {
                    LOG.info("wait for re-try: timeout = " + retryWaitTime);
                    Thread.sleep(retryWaitTime);
                }
                catch (InterruptedException e) {
                    // ignore
                }
            }
        }

        if (file == null) {
            throw new ClientException("cannot write job result: file is null.");
        }

        // return the data in the temp file
        try {
            LOG.info("read the result to file: path = "
                    + file.getAbsolutePath());
            InputStream fin = new BufferedInputStream(new GZIPInputStream(
                    new FileInputStream(file)));
            return new ExtUnpacker(file, new MessagePack().createUnpacker(fin));
        }
        catch (IOException e) {
            throw new ClientException(e);
        }
    }

    private File writeJobResultToTempFile(Job job)
            throws ClientException,
            IOException
    {
        GetJobResultRequest request = new GetJobResultRequest(new JobResult2(
                job));
        GetJobResultResult result = client.getJobResult(request);

        // download data of job result and write it to temp file
        long readSize = 0;
        long resultSize = result.getJobResult().getResultSize();
        LOG.info("check the size of the job result: size = " + resultSize);
        InputStream rin = new BufferedInputStream(
                ((JobResult2) result.getJobResult()).getResultInputStream());
        File file = null;
        OutputStream fout = null;
        try {
            file = File.createTempFile("td-jdbc-", ".tmp");
            LOG.info("created temp file: " + file.getAbsolutePath());
            fout = new BufferedOutputStream(new FileOutputStream(file));
            byte[] buf = new byte[1024];
            int len;
            while ((len = rin.read(buf)) != -1) {
                readSize += len;
                fout.write(buf, 0, len);
            }
            fout.flush();

            LOG.info("read the size of the job result: " + readSize);
            if (readSize < resultSize) {
                throw new IOException("Cannot read all data of the job result");
            }

            LOG.info("finished writing file");
            return file;
        }
        finally {
            if (fout != null) {
                try {
                    fout.close();
                }
                catch (IOException e) {
                    // ignore
                }
            }
            if (rin != null) {
                try {
                    rin.close();
                }
                catch (IOException e) {
                    // ignore
                }
            }
        }
    }

    public boolean flush()
    {
        return true;
    }

    public void close()
            throws ClientException
    {
    }
}
