package com.treasure_data.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.msgpack.MessagePack;
import org.msgpack.packer.BufferPacker;
import org.msgpack.type.ValueFactory;
import org.msgpack.unpacker.Unpacker;

import com.treasure_data.client.ClientException;
import com.treasure_data.jdbc.command.ClientAPI;
import com.treasure_data.model.Database;
import com.treasure_data.model.Job;
import com.treasure_data.model.JobSummary;
import com.treasure_data.model.JobSummary.Status;
import com.treasure_data.model.TableSummary;

public class TestTDResultSet {

    public static class MockClientAPI implements ClientAPI {
        public List<TableSummary> showTables() throws ClientException {
            return null;
        }

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

        public TDResultSetBase select(String sql) throws ClientException {
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
    public void testNext01() throws Exception {
        ClientAPI clientApi = new MockClientAPI() {
            public JobSummary waitJobResult(Job job) throws ClientException {
                String resultSchema = "[[\"age\",\"int\"],[\"name\",\"string\"]]";
                return new JobSummary("12345", JobSummary.Type.HIVE, new Database("mugadb"),
                        "url", "rtbl", Status.SUCCESS, "startAt", "endAt", "query", resultSchema);
            }

            public Unpacker getJobResult(Job job) throws ClientException {
                try {
                    MessagePack msgpack = new MessagePack();
                    BufferPacker packer = msgpack.createBufferPacker();
                    List<Object> ret0 = new ArrayList<Object>();
                    ret0.add(1);
                    ret0.add("muga");
                    packer.write(ret0);
                    List<Object> ret1 = new ArrayList<Object>();
                    ret1.add(2);
                    ret1.add("nishizawa");
                    packer.write(ret1);
                    byte[] bytes = packer.toByteArray();
                    return msgpack.createBufferUnpacker(bytes);
                } catch (java.io.IOException e) {
                    throw new ClientException("mock");
                }
            }
        };
        Job job = new Job("12345");
        ResultSet rs = new TDResultSet(clientApi, 50, job);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("muga", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("nishizawa", rs.getString(2));
        assertFalse(rs.next());
    }

    @Test
    public void testNext02() throws Exception {
        ClientAPI clientApi = new MockClientAPI() {
            public JobSummary waitJobResult(Job job) throws ClientException {
                String resultSchema = "[[\"age\",\"int\"],[\"name\",\"string\"]]";
                return new JobSummary("12345", JobSummary.Type.HIVE, new Database("mugadb"),
                        "url", "rtbl", Status.SUCCESS, "startAt", "endAt", "query", resultSchema);
            }

            public Unpacker getJobResult(Job job) throws ClientException {
                try {
                    MessagePack msgpack = new MessagePack();
                    BufferPacker packer = msgpack.createBufferPacker();
                    List<Object> ret0 = new ArrayList<Object>();
                    ret0.add(10);
                    ret0.add("muga");
                    packer.write(ret0);
                    byte[] bytes = packer.toByteArray();
                    return msgpack.createBufferUnpacker(bytes);
                } catch (java.io.IOException e) {
                    throw new ClientException("mock");
                }
            }
        };
        Job job = new Job("12345");
        ResultSet rs = new TDResultSet(clientApi, 50, job);
        assertTrue(rs.next());
        { // ok: int to int
            assertEquals(10, rs.getInt(1));
        }
        { // ok: int to object
            assertEquals(ValueFactory.createIntegerValue(10), rs.getObject(1));
        }
        { // error: int to string
            try {
                rs.getString(1);
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
        }
        { // error: int to boolean
            try {
                rs.getBoolean(1);
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
        }
    }

    @Test
    public void testNext03() throws Exception {
        final int count = 100;
        ClientAPI clientApi = new MockClientAPI() {
            public JobSummary waitJobResult(Job job) throws ClientException {
                String resultSchema = "[[\"p1\",\"string\"],[\"p2\",\"string\"]]";
                return new JobSummary("12345", JobSummary.Type.HIVE, new Database("mugadb"),
                        "url", "rtbl", Status.SUCCESS, "startAt", "endAt", "query", resultSchema);
            }

            public Unpacker getJobResult(Job job) throws ClientException {
                try {
                    MessagePack msgpack = new MessagePack();
                    BufferPacker packer = msgpack.createBufferPacker();
                    for (int i = 0; i < count; i++) {
                        List<Object> ret = new ArrayList<Object>();
                        ret.add("p1:" + i);
                        ret.add("p2:" + i);
                        packer.write(ret);
                    }
                    byte[] bytes = packer.toByteArray();
                    return msgpack.createBufferUnpacker(bytes);
                } catch (java.io.IOException e) {
                    throw new ClientException("mock");
                }
            }
        };

        Job job = new Job("12345");
        ResultSet rs = new TDResultSet(clientApi, 100, job);
        for (int i = 0; i < count; i++) {
            System.out.println("i: " + i);
            assertTrue(rs.next());
            assertEquals("p1:" + i, rs.getString(1));
            assertEquals("p2:" + i, rs.getString(2));
        }
        assertFalse(rs.next());
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
        { // getColumnType(int)
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
        { // getColumnTypeName(int)
            try {
                rsMetaData.getColumnTypeName(0);
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            assertEquals(Constants.STRING_TYPE_NAME, rsMetaData.getColumnTypeName(1));
            assertEquals(Constants.FLOAT_TYPE_NAME, rsMetaData.getColumnTypeName(2));
            assertEquals(Constants.DOUBLE_TYPE_NAME, rsMetaData.getColumnTypeName(3));
            assertEquals(Constants.BOOLEAN_TYPE_NAME, rsMetaData.getColumnTypeName(4));
            try {
                rsMetaData.getColumnTypeName(5);
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
        }
        { // getColumnName(int)
            try {
                rsMetaData.getColumnName(0);
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof ArrayIndexOutOfBoundsException);
            }
            assertEquals("p1", rsMetaData.getColumnName(1));
            assertEquals("p2", rsMetaData.getColumnName(2));
            assertEquals("p3", rsMetaData.getColumnName(3));
            assertEquals("p4", rsMetaData.getColumnName(4));
            try {
                rsMetaData.getColumnName(5);
            } catch (Throwable t) {
                assertTrue(t instanceof IndexOutOfBoundsException);
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
        { // getColumnType(int)
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
        { // getColumnTypeName(int)
            try {
                rsMetaData.getColumnTypeName(0);
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            assertEquals(Constants.TINYINT_TYPE_NAME, rsMetaData.getColumnTypeName(1));
            assertEquals(Constants.SMALLINT_TYPE_NAME, rsMetaData.getColumnTypeName(2));
            assertEquals(Constants.INT_TYPE_NAME, rsMetaData.getColumnTypeName(3));
            assertEquals(Constants.BIGINT_TYPE_NAME, rsMetaData.getColumnTypeName(4));
            try {
                rsMetaData.getColumnTypeName(5);
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
        }
        { // getColumnName(int)
            try {
                rsMetaData.getColumnName(0);
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof ArrayIndexOutOfBoundsException);
            }
            assertEquals("p1", rsMetaData.getColumnName(1));
            assertEquals("p2", rsMetaData.getColumnName(2));
            assertEquals("p3", rsMetaData.getColumnName(3));
            assertEquals("p4", rsMetaData.getColumnName(4));
            try {
                rsMetaData.getColumnName(5);
            } catch (Throwable t) {
                assertTrue(t instanceof IndexOutOfBoundsException);
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
        { // getColumnType(int)
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
        { // getColumnTypeName(int)
            try {
                rsMetaData.getColumnTypeName(0);
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            assertEquals(Constants.STRING_TYPE_NAME, rsMetaData.getColumnTypeName(1));
            assertEquals(Constants.STRING_TYPE_NAME, rsMetaData.getColumnTypeName(2));
            assertEquals(Constants.STRING_TYPE_NAME, rsMetaData.getColumnTypeName(3));
            try {
                rsMetaData.getColumnTypeName(4);
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
        }
        { // getColumnName(int)
            try {
                rsMetaData.getColumnName(0);
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof ArrayIndexOutOfBoundsException);
            }
            assertEquals("p1", rsMetaData.getColumnName(1));
            assertEquals("p2", rsMetaData.getColumnName(2));
            assertEquals("p3", rsMetaData.getColumnName(3));
            try {
                rsMetaData.getColumnName(4);
            } catch (Throwable t) {
                assertTrue(t instanceof IndexOutOfBoundsException);
            }
        }
    }
}
