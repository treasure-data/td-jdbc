package com.treasure_data.jdbc.command;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import org.msgpack.MessagePack;
import org.msgpack.unpacker.Unpacker;

import com.treasure_data.client.ClientException;
import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.jdbc.TDConnection;
import com.treasure_data.jdbc.TDResultSet;
import com.treasure_data.jdbc.TDResultSetBase;
import com.treasure_data.jdbc.Config;
import com.treasure_data.logger.TreasureDataLogger;
import com.treasure_data.model.AuthenticateRequest;
import com.treasure_data.model.Database;
import com.treasure_data.model.DatabaseSummary;
import com.treasure_data.model.GetJobResultRequest;
import com.treasure_data.model.GetJobResultResult;
import com.treasure_data.model.Job;
import com.treasure_data.model.JobResult;
import com.treasure_data.model.JobResult2;
import com.treasure_data.model.JobSummary;
import com.treasure_data.model.ShowJobRequest;
import com.treasure_data.model.ShowJobResult;
import com.treasure_data.model.SubmitJobRequest;
import com.treasure_data.model.SubmitJobResult;
import com.treasure_data.model.TableSummary;

public class TDClientAPI implements ClientAPI {
    private static final Logger LOG = Logger.getLogger(TDClientAPI.class.getName());

    private TreasureDataClient client;

    private Properties props;

    private Database database;

    private int maxRows = 5000;

    public TDClientAPI(TDConnection conn) {
        this(new TreasureDataClient(conn.getProperties()), conn.getProperties(),
                conn.getDatabase(), conn.getMaxRows());
    }

    public TDClientAPI(TreasureDataClient client, Properties props, Database database) {
        this(client, props, database, 5000);
    }

    public TDClientAPI(TreasureDataClient client, Properties props, Database database, int maxRows) {
        this.client = client;
        this.props = props;
        this.database = database;
        this.maxRows = maxRows;

        checkCredentials();

        {
            Properties sysprops = System.getProperties();
            if (sysprops.getProperty(Config.TD_LOGGER_AGENTMODE) == null) {
                sysprops.setProperty(Config.TD_LOGGER_AGENTMODE, "false");
            }
            if (sysprops.getProperty(Config.TD_LOGGER_API_KEY) == null) {
                String apiKey = client.getTreasureDataCredentials().getAPIKey();
                sysprops.setProperty(Config.TD_LOGGER_API_KEY, apiKey);
            }
        }
    }

    private void checkCredentials() {
        String apiKey = client.getTreasureDataCredentials().getAPIKey();
        if (apiKey != null) {
            return;
        }

        if (props == null) {
            return;
        }

        String user = props.getProperty("user");
        String password = props.getProperty("password");
        try {
            client.authenticate(new AuthenticateRequest(user, password));
        } catch (ClientException e) {
            LOG.throwing(this.getClass().getName(), "checkCredentials", e);
            return;
        }
    }

    public List<DatabaseSummary> showDatabases() throws ClientException {
        return client.listDatabases();
    }

    public List<TableSummary> showTables() throws ClientException {
        return client.listTables(database);
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

    public TDResultSetBase select(String sql) throws ClientException {
        return select(sql, 0);
    }

    public TDResultSetBase select(String sql, int queryTimeout)
            throws ClientException {
        TDResultSetBase rs = null;

        Job job = new Job(database, sql);
        SubmitJobRequest request = new SubmitJobRequest(job);
        SubmitJobResult result = client.submitJob(request);
        job = result.getJob();

        if (job != null) {
            rs = new TDResultSet(this, maxRows, job, queryTimeout);
        }
        return rs;
    }

    public JobSummary waitJobResult(Job job) throws ClientException {
        String jobID = job.getJobID();

        ShowJobResult result;
        while (true) {
            ShowJobRequest request = new ShowJobRequest(job);
            result = client.showJob(request);

            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("Job status: " + result.getJob().getStatus());
            }

            JobSummary.Status stat = result.getJob().getStatus();
            if (stat == JobSummary.Status.SUCCESS) {
                break;
            } else if (stat == JobSummary.Status.ERROR) {
                throw new ClientException("job error: job = " + jobID);
            } else if (stat == JobSummary.Status.KILLED) {
                throw new ClientException("job killed: job = " + jobID);
            }

            try {
                Thread.sleep(2 * 1000);
            } catch (InterruptedException e) {
                return null;
            }
        }
        return result.getJob();
    }

    public Unpacker getJobResult(Job job) throws ClientException {
        GetJobResultRequest request = new GetJobResultRequest(new JobResult(job));
        GetJobResultResult result = client.getJobResult(request);
        return result.getJobResult().getResult();
    }

    public ExtUnpacker getJobResult2(Job job) throws ClientException {
        File file = null;

        int retryCount = 0;
        int retryCountThreshold = Integer.parseInt(props.getProperty(
                Config.TD_JDBC_RESULT_RETRYCOUNT_THRESHOLD,
                Config.TD_JDBC_RESULT_RETRYCOUNT_THRESHOLD_DEFAULTVALUE));
        while (true) {
            try {
                LOG.info("write the result to file");
                file = writeJobResultToTempFile(job);
                break;
            } catch (Throwable t) {
                LOG.warning("cought exception: message = " + t.getMessage());
                t.printStackTrace();

                // catch ClientException, IOException
                if (!(retryCount < retryCountThreshold)) {
                    throw new ClientException(
                            "re-try out writing: threashold = "
                                    + retryCountThreshold);
                }

                retryCount++;
                LOG.info("re-try writing: imcremented retryCount = " + retryCount);
                long retryWaitTime = Long.parseLong(props.getProperty(
                        Config.TD_JDBC_RESULT_RETRY_WAITTIME,
                        Config.TD_JDBC_RESULT_RETRY_WAITTIME_DEFAULTVALUE));
                try {
                    LOG.info("wait for re-try: timeout = " + retryWaitTime);
                    Thread.sleep(retryWaitTime);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        }

        if (file == null) {
            throw new ClientException("cannot write job result: file is null.");
        }

        // return the data in the temp file
        try {
            LOG.info("read the result to file: paht = "
                    + file.getAbsolutePath());
            InputStream fin = new GZIPInputStream(new BufferedInputStream(
                    new FileInputStream(file)));
            return new ExtUnpacker(file, new MessagePack()
                    .createUnpacker(new BufferedInputStream(fin)));
        } catch (IOException e) {
            throw new ClientException(e);
        }
    }

    private File writeJobResultToTempFile(Job job) throws ClientException,
            IOException {
        GetJobResultRequest request = new GetJobResultRequest(new JobResult2(job));
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
        } finally {
            if (fout != null) {
                try {
                    fout.close();
                } catch (IOException e) {
                    // ignore
                }
            }
            if (rin != null) {
                try {
                    rin.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }

    public boolean flush() {
        TreasureDataLogger logger = TreasureDataLogger.getLogger(database.getName());
        logger.flush();
        return true;
    }

    public void close() throws ClientException {
        TreasureDataLogger logger = TreasureDataLogger.getLogger(database.getName());
        logger.flush();
        logger.close();
    }
}
