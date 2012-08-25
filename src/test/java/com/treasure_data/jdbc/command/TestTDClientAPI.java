package com.treasure_data.jdbc.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.client.ClientException;
import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.model.AuthenticateRequest;
import com.treasure_data.model.AuthenticateResult;
import com.treasure_data.model.Database;
import com.treasure_data.model.Job;
import com.treasure_data.model.JobSummary;
import com.treasure_data.model.ShowJobRequest;
import com.treasure_data.model.ShowJobResult;

public class TestTDClientAPI {

    @Test
    public void testWaitJobResult01() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties.default"));

        TreasureDataClient c = new TreasureDataClient() {
            @Override public AuthenticateResult authenticate(AuthenticateRequest request) {
                return null;
            }

            @Override public ShowJobResult showJob(ShowJobRequest request) {
                JobSummary js = new JobSummary("12345", Job.Type.HIVE, null, null, null,
                        JobSummary.Status.SUCCESS, null, null, "query", "rschema");
                return new ShowJobResult(js);
            }
        };
        TDClientAPI api = new TDClientAPI(c, new Database("mugadb"));

        Job job = new Job("12345");
        JobSummary js = api.waitJobResult(job);
        assertEquals("12345", js.getJobID());
        assertEquals("query", js.getQuery());
        assertEquals("rschema", js.getResultSchema());
    }

    @Test
    public void testWaitJobResult02() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties.default"));

        { // error occurred
            final String jobID = "12345";
            TreasureDataClient c = new TreasureDataClient() {
                @Override public AuthenticateResult authenticate(AuthenticateRequest request) {
                    return null;
                }

                @Override public ShowJobResult showJob(ShowJobRequest request) {
                    JobSummary js = new JobSummary(jobID, Job.Type.HIVE, null, null, null,
                            JobSummary.Status.ERROR, null, null, "query", "rschema");
                    return new ShowJobResult(js);
                }
            };
            TDClientAPI api = new TDClientAPI(c, new Database("mugadb"));

            try {
                Job job = new Job(jobID);
                JobSummary js = api.waitJobResult(job);
            } catch (Throwable t) {
                assertTrue(t instanceof ClientException);
            }
        }
        { // job are killed
            final String jobID = "12345";
            TreasureDataClient c = new TreasureDataClient() {
                @Override public AuthenticateResult authenticate(AuthenticateRequest request) {
                    return null;
                }

                @Override public ShowJobResult showJob(ShowJobRequest request) {
                    JobSummary js = new JobSummary(jobID, Job.Type.HIVE, null, null, null,
                            JobSummary.Status.KILLED, null, null, "query", "rschema");
                    return new ShowJobResult(js);
                }
            };
            TDClientAPI api = new TDClientAPI(c, new Database("mugadb"));

            try {
                Job job = new Job(jobID);
                JobSummary js = api.waitJobResult(job);
            } catch (Throwable t) {
                assertTrue(t instanceof ClientException);
            }
        }
    }

    @Test
    public void testWaitJobResult03() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties.default"));

        TreasureDataClient c = new TreasureDataClient() {
            private int count = 0;
            @Override public AuthenticateResult authenticate(AuthenticateRequest request) {
                return null;
            }

            @Override public ShowJobResult showJob(ShowJobRequest request) {
                if (count > 2) {
                    JobSummary js = new JobSummary("12345", Job.Type.HIVE, null, null, null,
                            JobSummary.Status.SUCCESS, null, null, "query", "rschema");
                    count++;
                    return new ShowJobResult(js);
                } else {
                    JobSummary js = new JobSummary("12345", Job.Type.HIVE, null, null, null,
                            JobSummary.Status.RUNNING, null, null, "query", "rschema");
                    count++;
                    return new ShowJobResult(js);
                }
            }
        };
        TDClientAPI api = new TDClientAPI(c, new Database("mugadb"));

        Job job = new Job("12345");
        JobSummary js = api.waitJobResult(job);
        assertEquals("12345", js.getJobID());
        assertEquals("query", js.getQuery());
        assertEquals("rschema", js.getResultSchema());
    }
}
