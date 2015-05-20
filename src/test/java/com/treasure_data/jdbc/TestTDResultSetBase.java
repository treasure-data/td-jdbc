package com.treasure_data.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.msgpack.MessagePack;
import org.msgpack.packer.BufferPacker;
import org.msgpack.unpacker.Unpacker;

import com.treasure_data.client.ClientException;
import com.treasure_data.jdbc.command.ClientAPI;
import com.treasure_data.jdbc.command.ClientAPI.ExtUnpacker;
import com.treasure_data.model.Database;
import com.treasure_data.model.DatabaseSummary;
import com.treasure_data.model.Job;
import com.treasure_data.model.JobSummary;
import com.treasure_data.model.JobSummary.Status;
import com.treasure_data.model.TableSummary;

public class TestTDResultSetBase {

    public static class MockClientAPI implements ClientAPI {
        public List<DatabaseSummary> showDatabases() throws ClientException {
            return null;
        }

        public DatabaseSummary showDatabase() throws ClientException {
            return null;
        }

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

        public TDResultSetBase select(String sql, int queryTimeout)
                throws ClientException {
            return null;
        }

        public TDResultSetMetaData getMetaDataWithSelect1() {
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

        public ExtUnpacker getJobResult2(Job job) throws ClientException {
            return new ExtUnpacker(null, getJobResult(job));
        }

        public void close() throws ClientException {
        }
    }

    public static class MockStringClientAPI extends MockClientAPI {
        private String name;

        public MockStringClientAPI(String name) {
            this.name = name;
        }

        public JobSummary waitJobResult(Job job) throws ClientException {
            String resultSchema = "[[\"name\",\"string\"]]";
            return new JobSummary("12345", JobSummary.Type.HIVE, new Database(
                    "mugadb"), "url", "rtbl", Status.SUCCESS, "startAt",
                    "endAt", "query", resultSchema);
        }

        public Unpacker getJobResult(Job job) throws ClientException {
            try {
                MessagePack msgpack = new MessagePack();
                BufferPacker packer = msgpack.createBufferPacker();
                List<Object> ret = new ArrayList<Object>();
                ret.add(name);
                packer.write(ret);
                byte[] bytes = packer.toByteArray();
                return msgpack.createBufferUnpacker(bytes);
            } catch (IOException e) {
                throw new ClientException("mock");
            }
        }
    }

    public static class MockIntegerClientAPI extends MockClientAPI {
        private int id;

        public MockIntegerClientAPI(int id) {
            this.id = id;
        }

        public JobSummary waitJobResult(Job job) throws ClientException {
            String resultSchema = "[[\"id\",\"int\"]]";
            return new JobSummary("12345", JobSummary.Type.HIVE, new Database(
                    "mugadb"), "url", "rtbl", Status.SUCCESS, "startAt",
                    "endAt", "query", resultSchema);
        }

        public Unpacker getJobResult(Job job) throws ClientException {
            try {
                MessagePack msgpack = new MessagePack();
                BufferPacker packer = msgpack.createBufferPacker();
                List<Object> ret = new ArrayList<Object>();
                ret.add(id);
                packer.write(ret);
                byte[] bytes = packer.toByteArray();
                return msgpack.createBufferUnpacker(bytes);
            } catch (IOException e) {
                throw new ClientException("mock");
            }
        }
    }

    public static class MockLongClientAPI extends MockClientAPI {
        private long id;

        public MockLongClientAPI(long id) {
            this.id = id;
        }

        public JobSummary waitJobResult(Job job) throws ClientException {
            String resultSchema = "[[\"id\",\"long\"]]";
            return new JobSummary("12345", JobSummary.Type.HIVE, new Database(
                    "mugadb"), "url", "rtbl", Status.SUCCESS, "startAt",
                    "endAt", "query", resultSchema);
        }

        public Unpacker getJobResult(Job job) throws ClientException {
            try {
                MessagePack msgpack = new MessagePack();
                BufferPacker packer = msgpack.createBufferPacker();
                List<Object> ret = new ArrayList<Object>();
                ret.add(id);
                packer.write(ret);
                byte[] bytes = packer.toByteArray();
                return msgpack.createBufferUnpacker(bytes);
            } catch (IOException e) {
                throw new ClientException("mock");
            }
        }
    }

    public static class MockDoubleClientAPI extends MockClientAPI {
        private double value;

        public MockDoubleClientAPI(double value) {
            this.value = value;
        }

        public JobSummary waitJobResult(Job job) throws ClientException {
            String resultSchema = "[[\"value\",\"double\"]]";
            return new JobSummary("12345", JobSummary.Type.HIVE, new Database(
                    "mugadb"), "url", "rtbl", Status.SUCCESS, "startAt",
                    "endAt", "query", resultSchema);
        }

        public Unpacker getJobResult(Job job) throws ClientException {
            try {
                MessagePack msgpack = new MessagePack();
                BufferPacker packer = msgpack.createBufferPacker();
                List<Object> ret = new ArrayList<Object>();
                ret.add(value);
                packer.write(ret);
                byte[] bytes = packer.toByteArray();
                return msgpack.createBufferUnpacker(bytes);
            } catch (IOException e) {
                throw new ClientException("mock");
            }
        }
    }

    /**
     * result: 0, 1, -1, Integer.MAX_VALUE, Integer.MIN_VALUE
     */
    @Test
    public void testImplicitIntTypeConversion01() throws Exception {
        int[] values = new int[] { 0, 1, -1, Integer.MAX_VALUE,
                Integer.MIN_VALUE, };

        for (int i = 0; i < values.length; i++) {
            int src = values[i];
            ClientAPI clientApi = new MockIntegerClientAPI(src);
            Job job = new Job("12345");
            ResultSet rs = new TDResultSet(clientApi, 50, job);
            assertTrue(rs.next());

            assertEquals((byte) src, rs.getByte(1)); // to byte type
            assertEquals((short) src, rs.getShort(1)); // to short type
            assertEquals(src, rs.getInt(1)); // to int type
            assertEquals((long) src, rs.getLong(1)); // to long type
            assertEquals((float) src, rs.getFloat(1), 0); // to float type
            assertEquals((double) src, rs.getDouble(1), 0); // to double type
            assertEquals("" + src, rs.getString(1)); // to String type

            assertFalse(rs.next());
        }
    }

    /**
     * result: 0, 1, -1, Long.MAX_VALUE, Long.MIN_VALUE
     */
    @Test
    public void testImplicitLongTypeConversion01() throws Exception {
        long[] values = new long[] { 0, 1, -1, Long.MAX_VALUE, Long.MIN_VALUE, };

        for (int i = 0; i < values.length; i++) {
            long src = values[i];
            ClientAPI clientApi = new MockLongClientAPI(src);
            Job job = new Job("12345");
            ResultSet rs = new TDResultSet(clientApi, 50, job);
            assertTrue(rs.next());

            assertEquals((byte) src, rs.getByte(1)); // to byte type
            assertEquals((short) src, rs.getShort(1)); // to short type
            assertEquals((int) src, rs.getInt(1)); // to int type
            assertEquals(src, rs.getLong(1)); // to long type
            assertEquals((float) src, rs.getFloat(1), 0); // to float type
            assertEquals((double) src, rs.getDouble(1), 0); // to double type
            assertEquals("" + src, rs.getString(1)); // to String type

            assertFalse(rs.next());
        }
    }

    /**
     * result: 0, 0.0, -0.0, 1, 1.0, -1, -1.0, Double.MAX_VALUE,
     * Double.MIN_VALUE
     */
    @Test
    public void testImplicitDoubleTypeConversion01() throws Exception {
        double[] values = new double[] { 0, 0.0, -0.0, 1, 1.0, -1, -1.0,
                Double.MAX_VALUE, Double.MIN_VALUE };

        for (int i = 0; i < values.length; i++) {
            double src = values[i];
            ClientAPI clientApi = new MockDoubleClientAPI(src);
            Job job = new Job("12345");
            ResultSet rs = new TDResultSet(clientApi, 50, job);
            assertTrue(rs.next());

            assertEquals((byte) src, rs.getByte(1)); // to byte type
            assertEquals((short) src, rs.getShort(1)); // to short type
            assertEquals((int) src, rs.getInt(1)); // to int type
            assertEquals((long) src, rs.getLong(1)); // to long type
            assertEquals((float) src, rs.getFloat(1), 0); // to float type
            assertEquals(src, rs.getDouble(1), 0); // to double type
            assertEquals("" + src, rs.getString(1)); // to String type

            assertFalse(rs.next());
        }
    }

    /**
     * result: "0", "0.0", "-0.0", "1", "1.0", "-1", "-1.0" Byte.MAX_VALUE,
     * Byte.MIN_VALUE, Short.MAX_VALUE, Short.MIN_VALUE Integer.MAX_VALUE,
     * Integer.MIN_VALUE, Long.MAX_VALUE, Long.MIN_VALUE
     */
    @Test
    public void testImplicitStringTypeConversion01() throws Exception {
        { // "0"
            String src = "0";
            ClientAPI clientApi = new MockStringClientAPI(src);
            Job job = new Job("12345");
            ResultSet rs = new TDResultSet(clientApi, 50, job);
            assertTrue(rs.next());

            assertEquals((byte) Byte.parseByte(src), rs.getByte(1)); // to byte
                                                                     // type
            assertEquals((short) Short.parseShort(src), rs.getShort(1)); // to
                                                                         // short
                                                                         // type
            assertEquals(Integer.parseInt(src), rs.getInt(1)); // to int type
            assertEquals(Long.parseLong(src), rs.getLong(1)); // to long type
            assertEquals(Float.parseFloat(src), rs.getFloat(1), 0); // to float
                                                                    // type
            assertEquals(Double.parseDouble(src), rs.getDouble(1), 0); // to
                                                                       // double
                                                                       // type
            assertEquals(src, rs.getString(1)); // to String type

            assertFalse(rs.next());
        }
        { // "0.0"
            String src = "0.0";
            ClientAPI clientApi = new MockStringClientAPI(src);
            Job job = new Job("12345");
            ResultSet rs = new TDResultSet(clientApi, 50, job);
            assertTrue(rs.next());

            try {
                rs.getByte(1); // to byte type
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            try {
                rs.getShort(1); // to short type
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            try {
                rs.getInt(1); // to int type
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            try {
                rs.getLong(1); // to long type
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            assertEquals(Float.parseFloat(src), rs.getFloat(1), 0); // to float
                                                                    // type
            assertEquals(Double.parseDouble(src), rs.getDouble(1), 0); // to
                                                                       // double
                                                                       // type
            assertEquals(src, rs.getString(1)); // to String type

            assertFalse(rs.next());
        }
        { // "-0.0"
            String src = "0.0";
            ClientAPI clientApi = new MockStringClientAPI(src);
            Job job = new Job("12345");
            ResultSet rs = new TDResultSet(clientApi, 50, job);
            assertTrue(rs.next());

            try {
                rs.getByte(1); // to byte type
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            try {
                rs.getShort(1); // to short type
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            try {
                rs.getInt(1); // to int type
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            try {
                rs.getLong(1); // to long type
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            assertEquals(Float.parseFloat(src), rs.getFloat(1), 0); // to float
                                                                    // type
            assertEquals(Double.parseDouble(src), rs.getDouble(1), 0); // to
                                                                       // double
                                                                       // type
            assertEquals(src, rs.getString(1)); // to String type

            assertFalse(rs.next());
        }
        { // "1"
            String src = "1";
            ClientAPI clientApi = new MockStringClientAPI(src);
            Job job = new Job("12345");
            ResultSet rs = new TDResultSet(clientApi, 50, job);
            assertTrue(rs.next());

            assertEquals((byte) Byte.parseByte(src), rs.getByte(1)); // to byte
                                                                     // type
            assertEquals((short) Short.parseShort(src), rs.getShort(1)); // to
                                                                         // short
                                                                         // type
            assertEquals(Integer.parseInt(src), rs.getInt(1)); // to int type
            assertEquals(Long.parseLong(src), rs.getLong(1)); // to long type
            assertEquals(Float.parseFloat(src), rs.getFloat(1), 0); // to float
                                                                    // type
            assertEquals(Double.parseDouble(src), rs.getDouble(1), 0); // to
                                                                       // double
                                                                       // type
            assertEquals(src, rs.getString(1)); // to String type

            assertFalse(rs.next());
        }
        { // "1.0"
            String src = "0.0";
            ClientAPI clientApi = new MockStringClientAPI(src);
            Job job = new Job("12345");
            ResultSet rs = new TDResultSet(clientApi, 50, job);
            assertTrue(rs.next());

            try {
                rs.getByte(1); // to byte type
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            try {
                rs.getShort(1); // to short type
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            try {
                rs.getInt(1); // to int type
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            try {
                rs.getLong(1); // to long type
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            assertEquals(Float.parseFloat(src), rs.getFloat(1), 0); // to float
                                                                    // type
            assertEquals(Double.parseDouble(src), rs.getDouble(1), 0); // to
                                                                       // double
                                                                       // type
            assertEquals(src, rs.getString(1)); // to String type

            assertFalse(rs.next());
        }
        { // "-1"
            String src = "-1";
            ClientAPI clientApi = new MockStringClientAPI(src);
            Job job = new Job("12345");
            ResultSet rs = new TDResultSet(clientApi, 50, job);
            assertTrue(rs.next());

            assertEquals((byte) Byte.parseByte(src), rs.getByte(1)); // to byte
                                                                     // type
            assertEquals((short) Short.parseShort(src), rs.getShort(1)); // to
                                                                         // short
                                                                         // type
            assertEquals(Integer.parseInt(src), rs.getInt(1)); // to int type
            assertEquals(Long.parseLong(src), rs.getLong(1)); // to long type
            assertEquals(Float.parseFloat(src), rs.getFloat(1), 0); // to float
                                                                    // type
            assertEquals(Double.parseDouble(src), rs.getDouble(1), 0); // to
                                                                       // double
                                                                       // type
            assertEquals(src, rs.getString(1)); // to String type

            assertFalse(rs.next());
        }
        { // "-1.0"
            String src = "-1.0";
            ClientAPI clientApi = new MockStringClientAPI(src);
            Job job = new Job("12345");
            ResultSet rs = new TDResultSet(clientApi, 50, job);
            assertTrue(rs.next());

            try {
                rs.getByte(1); // to byte type
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            try {
                rs.getShort(1); // to short type
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            try {
                rs.getInt(1); // to int type
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            try {
                rs.getLong(1); // to long type
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            assertEquals(Float.parseFloat(src), rs.getFloat(1), 0); // to float
                                                                    // type
            assertEquals(Double.parseDouble(src), rs.getDouble(1), 0); // to
                                                                       // double
                                                                       // type
            assertEquals(src, rs.getString(1)); // to String type

            assertFalse(rs.next());
        }
        { // Byte.MAX_VALUE
            String src = "" + Byte.MAX_VALUE;
            ClientAPI clientApi = new MockStringClientAPI(src);
            Job job = new Job("12345");
            ResultSet rs = new TDResultSet(clientApi, 50, job);
            assertTrue(rs.next());

            assertEquals((byte) Byte.parseByte(src), rs.getByte(1)); // to byte
                                                                     // type
            assertEquals((short) Short.parseShort(src), rs.getShort(1)); // to
                                                                         // short
                                                                         // type
            assertEquals(Integer.parseInt(src), rs.getInt(1)); // to int type
            assertEquals(Long.parseLong(src), rs.getLong(1)); // to long type
            assertEquals(Float.parseFloat(src), rs.getFloat(1), 0); // to float
                                                                    // type
            assertEquals(Double.parseDouble(src), rs.getDouble(1), 0); // to
                                                                       // double
                                                                       // type
            assertEquals(src, rs.getString(1)); // to String type

            assertFalse(rs.next());
        }
        { // Byte.MIN_VALUE
            String src = "" + Byte.MIN_VALUE;
            ClientAPI clientApi = new MockStringClientAPI(src);
            Job job = new Job("12345");
            ResultSet rs = new TDResultSet(clientApi, 50, job);
            assertTrue(rs.next());

            assertEquals((byte) Byte.parseByte(src), rs.getByte(1)); // to byte
                                                                     // type
            assertEquals((short) Short.parseShort(src), rs.getShort(1)); // to
                                                                         // short
                                                                         // type
            assertEquals(Integer.parseInt(src), rs.getInt(1)); // to int type
            assertEquals(Long.parseLong(src), rs.getLong(1)); // to long type
            assertEquals(Float.parseFloat(src), rs.getFloat(1), 0); // to float
                                                                    // type
            assertEquals(Double.parseDouble(src), rs.getDouble(1), 0); // to
                                                                       // double
                                                                       // type
            assertEquals(src, rs.getString(1)); // to String type

            assertFalse(rs.next());
        }
        { // Short.MAX_VALUE
            String src = "" + Short.MAX_VALUE;
            ClientAPI clientApi = new MockStringClientAPI(src);
            Job job = new Job("12345");
            ResultSet rs = new TDResultSet(clientApi, 50, job);
            assertTrue(rs.next());

            try {
                rs.getByte(1); // to byte type
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            assertEquals((short) Short.parseShort(src), rs.getShort(1)); // to
                                                                         // short
                                                                         // type
            assertEquals(Integer.parseInt(src), rs.getInt(1)); // to int type
            assertEquals(Long.parseLong(src), rs.getLong(1)); // to long type
            assertEquals(Float.parseFloat(src), rs.getFloat(1), 0); // to float
                                                                    // type
            assertEquals(Double.parseDouble(src), rs.getDouble(1), 0); // to
                                                                       // double
                                                                       // type
            assertEquals(src, rs.getString(1)); // to String type

            assertFalse(rs.next());
        }
        { // Short.MIN_VALUE
            String src = "" + Short.MIN_VALUE;
            ClientAPI clientApi = new MockStringClientAPI(src);
            Job job = new Job("12345");
            ResultSet rs = new TDResultSet(clientApi, 50, job);
            assertTrue(rs.next());

            try {
                rs.getByte(1); // to byte type
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            assertEquals((short) Short.parseShort(src), rs.getShort(1)); // to
                                                                         // short
                                                                         // type
            assertEquals(Integer.parseInt(src), rs.getInt(1)); // to int type
            assertEquals(Long.parseLong(src), rs.getLong(1)); // to long type
            assertEquals(Float.parseFloat(src), rs.getFloat(1), 0); // to float
                                                                    // type
            assertEquals(Double.parseDouble(src), rs.getDouble(1), 0); // to
                                                                       // double
                                                                       // type
            assertEquals(src, rs.getString(1)); // to String type

            assertFalse(rs.next());
        }
        { // Integer.MAX_VALUE
            String src = "" + Integer.MIN_VALUE;
            ClientAPI clientApi = new MockStringClientAPI(src);
            Job job = new Job("12345");
            ResultSet rs = new TDResultSet(clientApi, 50, job);
            assertTrue(rs.next());

            try {
                rs.getByte(1); // to byte type
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            try {
                rs.getShort(1); // to short type
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            assertEquals(Integer.parseInt(src), rs.getInt(1)); // to int type
            assertEquals(Long.parseLong(src), rs.getLong(1)); // to long type
            assertEquals(Float.parseFloat(src), rs.getFloat(1), 0); // to float
                                                                    // type
            assertEquals(Double.parseDouble(src), rs.getDouble(1), 0); // to
                                                                       // double
                                                                       // type
            assertEquals(src, rs.getString(1)); // to String type

            assertFalse(rs.next());
        }
        { // Integer.MIN_VALUE
            String src = "" + Integer.MIN_VALUE;
            ClientAPI clientApi = new MockStringClientAPI(src);
            Job job = new Job("12345");
            ResultSet rs = new TDResultSet(clientApi, 50, job);
            assertTrue(rs.next());

            try {
                rs.getByte(1); // to byte type
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            try {
                rs.getShort(1); // to short type
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            assertEquals(Integer.parseInt(src), rs.getInt(1)); // to int type
            assertEquals(Long.parseLong(src), rs.getLong(1)); // to long type
            assertEquals(Float.parseFloat(src), rs.getFloat(1), 0); // to float
                                                                    // type
            assertEquals(Double.parseDouble(src), rs.getDouble(1), 0); // to
                                                                       // double
                                                                       // type
            assertEquals(src, rs.getString(1)); // to String type

            assertFalse(rs.next());
        }
        { // Long.MAX_VALUE
            String src = "" + Long.MAX_VALUE;
            ClientAPI clientApi = new MockStringClientAPI(src);
            Job job = new Job("12345");
            ResultSet rs = new TDResultSet(clientApi, 50, job);
            assertTrue(rs.next());

            try {
                rs.getByte(1); // to byte type
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            try {
                rs.getShort(1); // to short type
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            try {
                rs.getInt(1); // to int type
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            assertEquals(Long.parseLong(src), rs.getLong(1)); // to long type
            assertEquals(Float.parseFloat(src), rs.getFloat(1), 0); // to float
                                                                    // type
            assertEquals(Double.parseDouble(src), rs.getDouble(1), 0); // to
                                                                       // double
                                                                       // type
            assertEquals(src, rs.getString(1)); // to String type

            assertFalse(rs.next());
        }
        { // Long.MIN_VALUE
            String src = "" + Long.MIN_VALUE;
            ClientAPI clientApi = new MockStringClientAPI(src);
            Job job = new Job("12345");
            ResultSet rs = new TDResultSet(clientApi, 50, job);
            assertTrue(rs.next());

            try {
                rs.getByte(1); // to byte type
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            try {
                rs.getShort(1); // to short type
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            try {
                rs.getInt(1); // to int type
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            assertEquals(Long.parseLong(src), rs.getLong(1)); // to long type
            assertEquals(Float.parseFloat(src), rs.getFloat(1), 0); // to float
                                                                    // type
            assertEquals(Double.parseDouble(src), rs.getDouble(1), 0); // to
                                                                       // double
                                                                       // type
            assertEquals(src, rs.getString(1)); // to String type

            assertFalse(rs.next());
        }
    }

    /**
     * result: "true", "false", "muga"
     */
    @Test
    public void testImplicitStringTypeConversion02() throws Exception {
        { // "true"
            ClientAPI clientApi = new MockStringClientAPI("true");
            Job job = new Job("12345");
            ResultSet rs = new TDResultSet(clientApi, 50, job);
            assertTrue(rs.next());
            // to boolean type
            assertTrue(rs.getBoolean(1));
            // to String type
            assertEquals("true", rs.getString(1));
            assertFalse(rs.next());
        }
        { // "false"
            ClientAPI clientApi = new MockStringClientAPI("false");
            Job job = new Job("12345");
            ResultSet rs = new TDResultSet(clientApi, 50, job);
            assertTrue(rs.next());
            // to boolean type
            assertTrue(!rs.getBoolean(1));
            // to String type
            assertEquals("false", rs.getString(1));
            assertFalse(rs.next());
        }
        { // "muga"
            ClientAPI clientApi = new MockStringClientAPI("muga");
            Job job = new Job("12345");
            ResultSet rs = new TDResultSet(clientApi, 50, job);
            assertTrue(rs.next());
            // to boolean type
            assertTrue(rs.getBoolean(1));
            // to String type
            assertEquals("muga", rs.getString(1));
            assertFalse(rs.next());
        }
    }
}
