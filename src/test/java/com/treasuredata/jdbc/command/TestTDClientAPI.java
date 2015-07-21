package com.treasuredata.jdbc.command;

import com.treasure_data.client.ClientException;
import com.treasure_data.client.TreasureDataClient;
import com.treasuredata.jdbc.Config;
import com.treasuredata.jdbc.ConfigBuilder;
import com.treasuredata.jdbc.TDResultSetMetaData;
import com.treasure_data.model.AuthenticateRequest;
import com.treasure_data.model.AuthenticateResult;
import com.treasure_data.model.Database;
import com.treasure_data.model.Job;
import com.treasure_data.model.JobSummary;
import com.treasure_data.model.JobSummary.Debug;
import com.treasure_data.model.ShowJobRequest;
import com.treasure_data.model.ShowJobResult;
import com.treasure_data.model.ShowJobStatusRequest;
import com.treasure_data.model.ShowJobStatusResult;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestTDClientAPI
{
    private static Config newTestConfig()
            throws SQLException
    {
        ConfigBuilder configBuilder = new ConfigBuilder();
        configBuilder.setUser("xxxx");
        configBuilder.setPassword("xxxx");
        //props.setProperty("td.logger.api.key", "xxxx");
        return configBuilder.createConnectionConfig();
    }

    @Test
    public void testWaitJobResult01()
            throws Exception
    {
        Config config = newTestConfig();
        TreasureDataClient c = new TreasureDataClient(config.toProperties())
        {
            @Override
            public AuthenticateResult authenticate(AuthenticateRequest request)
            {
                return null;
            }

            @Override
            public ShowJobStatusResult showJobStatus(ShowJobStatusRequest request)
            {
                return new ShowJobStatusResult(JobSummary.Status.SUCCESS);
            }

            @Override
            public ShowJobResult showJob(ShowJobRequest request)
            {
                JobSummary js = new JobSummary("12345", Job.Type.HIVE, null, null, null,
                        JobSummary.Status.SUCCESS, null, null, "query", "rschema");
                return new ShowJobResult(js);
            }
        };
        TDClientAPI api = new TDClientAPI(newTestConfig(), c, new Database("mugadb"));

        Job job = new Job("12345");
        JobSummary js = api.waitJobResult(job);
        assertEquals("12345", js.getJobID());
        assertEquals("query", js.getQuery());
        assertEquals("rschema", js.getResultSchema());
    }

    @Test
    public void testWaitJobResult02()
            throws Exception
    {
        Config config = newTestConfig();
        { // error occurred
            final String jobID = "12345";
            TreasureDataClient c = new TreasureDataClient(config.toProperties())
            {
                @Override
                public AuthenticateResult authenticate(AuthenticateRequest request)
                {
                    return null;
                }

                @Override
                public ShowJobStatusResult showJobStatus(ShowJobStatusRequest request)
                {
                    return new ShowJobStatusResult(JobSummary.Status.ERROR);
                }

                @Override
                public ShowJobResult showJob(ShowJobRequest request)
                {
                    JobSummary js = new JobSummary(jobID, Job.Type.HIVE, new Database("mugadb"),
                            "url", "resultTable", JobSummary.Status.ERROR, "startAt", "endAt",
                            "query", "rschema", new Debug("cmdout", "stderr"));
                    return new ShowJobResult(js);
                }
            };
            TDClientAPI api = new TDClientAPI(config, c, new Database("mugadb"));

            try {
                Job job = new Job(jobID);
                JobSummary js = api.waitJobResult(job);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof ClientException);
            }
        }
        { // job are killed
            final String jobID = "12345";
            TreasureDataClient c = new TreasureDataClient(config.toProperties())
            {
                @Override
                public AuthenticateResult authenticate(AuthenticateRequest request)
                {
                    return null;
                }

                @Override
                public ShowJobStatusResult showJobStatus(ShowJobStatusRequest request)
                {
                    return new ShowJobStatusResult(JobSummary.Status.ERROR);
                }

                @Override
                public ShowJobResult showJob(ShowJobRequest request)
                {
                    JobSummary js = new JobSummary(jobID, Job.Type.HIVE, new Database("mugadb"),
                            "url", "resultTable", JobSummary.Status.ERROR, "startAt", "endAt",
                            "query", "rschema", new Debug("cmdout", "stderr"));
                    return new ShowJobResult(js);
                }
            };
            TDClientAPI api = new TDClientAPI(config, c, new Database("mugadb"));

            try {
                Job job = new Job(jobID);
                JobSummary js = api.waitJobResult(job);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof ClientException);
            }
        }
    }

    @Test
    public void testWaitJobResult03()
            throws Exception
    {
        Config config = newTestConfig();
        TreasureDataClient c = new TreasureDataClient(config.toProperties())
        {
            private int count = 0;

            @Override
            public AuthenticateResult authenticate(AuthenticateRequest request)
            {
                return null;
            }

            @Override
            public ShowJobStatusResult showJobStatus(ShowJobStatusRequest request)
            {
                if (count > 2) {
                    count++;
                    return new ShowJobStatusResult(JobSummary.Status.SUCCESS);
                }
                else {
                    count++;
                    return new ShowJobStatusResult(JobSummary.Status.RUNNING);
                }
            }

            @Override
            public ShowJobResult showJob(ShowJobRequest request)
            {
                JobSummary js = new JobSummary("12345", Job.Type.HIVE, null, null, null,
                        JobSummary.Status.SUCCESS, null, null, "query", "rschema");
                return new ShowJobResult(js);
            }
        };
        TDClientAPI api = new TDClientAPI(config, c, new Database("mugadb"));

        Job job = new Job("12345");
        JobSummary js = api.waitJobResult(job);
        assertEquals("12345", js.getJobID());
        assertEquals("query", js.getQuery());
        assertEquals("rschema", js.getResultSchema());
    }

    @Test
    public void testMetaDataWithSelect1()
            throws Exception
    {
        Config config = newTestConfig();
        TreasureDataClient c = new TreasureDataClient(config.toProperties())
        {
            @Override
            public AuthenticateResult authenticate(AuthenticateRequest request)
            {
                return null;
            }

            @Override
            public ShowJobStatusResult showJobStatus(ShowJobStatusRequest request)
            {
                return new ShowJobStatusResult(JobSummary.Status.SUCCESS);
            }

            @Override
            public ShowJobResult showJob(ShowJobRequest request)
            {
                JobSummary js = new JobSummary("12345", Job.Type.HIVE, null, null, null,
                        JobSummary.Status.SUCCESS, null, null, "query", "rschema");
                return new ShowJobResult(js);
            }
        };

        { // hive
            Config newConfig = new ConfigBuilder(config).setType(Job.Type.HIVE).createConnectionConfig();
            TDClientAPI api = new TDClientAPI(newConfig, c, new Database("mugadb"));

            TDResultSetMetaData md = api.getMetaDataWithSelect1();
            assertEquals(1, md.getColumnCount());
            assertEquals("_c0", md.getColumnName(1));
            assertEquals(Types.INTEGER, md.getColumnType(1));
        }
        { // presto
            Config newConfig = new ConfigBuilder(config).setType(Job.Type.PRESTO).createConnectionConfig();
            TDClientAPI api = new TDClientAPI(newConfig, c, new Database("mugadb"));
            TDResultSetMetaData md = api.getMetaDataWithSelect1();
            assertEquals(1, md.getColumnCount());
            assertEquals("_col0", md.getColumnName(1));
            assertEquals(Types.BIGINT, md.getColumnType(1));
        }
    }
}
