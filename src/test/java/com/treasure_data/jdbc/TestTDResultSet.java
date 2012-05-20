package com.treasure_data.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONValue;
import org.junit.Test;
import org.msgpack.MessagePack;
import org.msgpack.packer.BufferPacker;
import org.msgpack.unpacker.Unpacker;

import com.treasure_data.client.ClientException;
import com.treasure_data.jdbc.command.ClientAPI;
import com.treasure_data.model.Database;
import com.treasure_data.model.Job;
import com.treasure_data.model.JobSummary;
import com.treasure_data.model.JobSummary.Status;

public class TestTDResultSet {

    public static class MockClientAPI implements ClientAPI {
        public boolean drop(String tableName) throws ClientException {
            return false;
        }

        public boolean create(String table) throws ClientException {
            return false;
        }

        public boolean insert(String tableName, Map<String, Object> record)
                throws ClientException {
            return false;
        }

        public ResultSet select(String sql) throws ClientException {
            return null;
        }

        public boolean flush() {
            return false;
        }

        public JobSummary waitJobResult(Job job) throws ClientException {
            return null;
        }

        public Unpacker getJobResult(Job job) throws ClientException {
            return null;
        }
    }

    @Test
    public void testGetMetaData01() throws Exception {
        ClientAPI clientApi = new MockClientAPI() {
            public JobSummary waitJobResult(Job job) throws ClientException {
                String resultSchema = "[[\"p1\",\"string\"],[\"p2\",\"float\"],[\"p3\",\"double\"],[\"p4\",\"boolean\"]]";
                return new JobSummary("12345", JobSummary.Type.HIVE, new Database("mugadb"),
                        "url", "rtbl", Status.SUCCESS, "startAt", "endAt", "query", resultSchema);
            }
        };
        Job job = new Job("12345");
        TDResultSet rs = new TDResultSet(clientApi, 50, job);
        ResultSetMetaData rsMetaData = rs.getMetaData();
        {
            try {
                rsMetaData.getColumnType(0);
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            assertEquals(Utils.hiveTypeToSqlType(Constants.STRING_TYPE_NAME), rsMetaData.getColumnType(1));
            assertEquals(Utils.hiveTypeToSqlType(Constants.FLOAT_TYPE_NAME), rsMetaData.getColumnType(2));
            assertEquals(Utils.hiveTypeToSqlType(Constants.DOUBLE_TYPE_NAME), rsMetaData.getColumnType(3));
            assertEquals(Utils.hiveTypeToSqlType(Constants.BOOLEAN_TYPE_NAME), rsMetaData.getColumnType(4));
            try {
                rsMetaData.getColumnType(5);
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
        }
    }

    @Test
    public void testGetMetaData02() throws Exception {
        ClientAPI clientApi = new MockClientAPI() {
            public JobSummary waitJobResult(Job job) throws ClientException {
                String resultSchema = "[[\"p1\",\"tinyint\"],[\"p2\",\"smallint\"],[\"p3\",\"int\"],[\"p4\",\"bigint\"]]";
                return new JobSummary("12345", JobSummary.Type.HIVE, new Database("mugadb"),
                        "url", "rtbl", Status.SUCCESS, "startAt", "endAt", "query", resultSchema);
            }
        };
        Job job = new Job("12345");
        TDResultSet rs = new TDResultSet(clientApi, 50, job);
        ResultSetMetaData rsMetaData = rs.getMetaData();
        {
            try {
                rsMetaData.getColumnType(0);
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            assertEquals(Utils.hiveTypeToSqlType(Constants.TINYINT_TYPE_NAME), rsMetaData.getColumnType(1));
            assertEquals(Utils.hiveTypeToSqlType(Constants.SMALLINT_TYPE_NAME), rsMetaData.getColumnType(2));
            assertEquals(Utils.hiveTypeToSqlType(Constants.INT_TYPE_NAME), rsMetaData.getColumnType(3));
            assertEquals(Utils.hiveTypeToSqlType(Constants.BIGINT_TYPE_NAME), rsMetaData.getColumnType(4));
            try {
                rsMetaData.getColumnType(5);
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
        }
    }

    @Test
    public void testGetMetaData03() throws Exception {
        ClientAPI clientApi = new MockClientAPI() {
            public JobSummary waitJobResult(Job job) throws ClientException {
                String resultSchema = "[[\"p1\",\"map<string,int>\"],[\"p2\",\"array<int>\"],[\"p3\",\"struct<int>\"]]";
                return new JobSummary("12345", JobSummary.Type.HIVE, new Database("mugadb"),
                        "url", "rtbl", Status.SUCCESS, "startAt", "endAt", "query", resultSchema);
            }
        };
        Job job = new Job("12345");
        TDResultSet rs = new TDResultSet(clientApi, 50, job);
        ResultSetMetaData rsMetaData = rs.getMetaData();
        {
            try {
                rsMetaData.getColumnType(0);
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            assertEquals(Utils.hiveTypeToSqlType(Constants.STRING_TYPE_NAME), rsMetaData.getColumnType(1));
            assertEquals(Utils.hiveTypeToSqlType(Constants.STRING_TYPE_NAME), rsMetaData.getColumnType(2));
            assertEquals(Utils.hiveTypeToSqlType(Constants.STRING_TYPE_NAME), rsMetaData.getColumnType(3));
            try {
                rsMetaData.getColumnType(4);
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
        }
    }

    @Test
    public void testSample() throws Exception {
        ClientAPI clientApi = new MockClientAPI() {
            public JobSummary waitJobResult(Job job) throws ClientException {
                String resultSchema = "[[\"age\",\"int\"],[\"name\",\"string\"]]";
                return new JobSummary("12345", JobSummary.Type.HIVE, new Database("mugadb"),
                        "url", "rtbl", Status.SUCCESS, "startAt", "endAt", "query", resultSchema);
            }

            public Unpacker getJobResult(Job job) throws ClientException {
                List<List<Object>> result = new ArrayList<List<Object>>();
                List<Object> ret0 = new ArrayList<Object>();
                ret0.add(10);
                ret0.add("muga");
                result.add(ret0);
                List<Object> ret1 = new ArrayList<Object>();
                ret1.add(20);
                ret1.add("nishizawa");
                result.add(ret1);

                try {
                    MessagePack msgpack = new MessagePack();
                    BufferPacker packer = msgpack.createBufferPacker();
                    packer.write(result);
                    byte[] bytes = packer.toByteArray();
                    return msgpack.createBufferUnpacker(bytes);
                } catch (java.io.IOException e) {
                    throw new ClientException("mock");
                }
            }
        };
        Job job = new Job("12345");
        ResultSet rs = new TDResultSet(clientApi, 50, job);
        while (rs.next()) {
            System.out.println(rs.getObject(1));
            System.out.println(rs.getObject(2));
        }
    }
}
