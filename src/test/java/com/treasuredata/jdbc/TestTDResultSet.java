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
import com.treasure_data.model.Database;
import com.treasure_data.model.DatabaseSummary;
import com.treasure_data.model.Job;
import com.treasure_data.model.JobSummary;
import com.treasure_data.model.JobSummary.Status;
import com.treasure_data.model.TableSummary;
import org.junit.Test;
import org.msgpack.MessagePack;
import org.msgpack.packer.BufferPacker;
import org.msgpack.type.ValueFactory;
import org.msgpack.unpacker.Unpacker;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestTDResultSet
{

    public static class MockClientAPI
            implements ClientAPI
    {
        public List<DatabaseSummary> showDatabases()
                throws ClientException
        {
            return null;
        }

        public DatabaseSummary showDatabase()
                throws ClientException
        {
            return null;
        }

        public List<TableSummary> showTables()
                throws ClientException
        {
            return null;
        }

        public boolean drop(String tableName)
                throws ClientException
        {
            return false;
        }

        public boolean create(String table)
                throws ClientException
        {
            return false;
        }

        public boolean insert(String tableName, Map<String, Object> record)
                throws ClientException
        {
            return false;
        }

        public TDResultSetBase select(String sql)
                throws ClientException
        {
            return null;
        }

        public TDResultSetBase select(String sql, int queryTimeout)
                throws ClientException
        {
            return null;
        }

        public TDResultSetMetaData getMetaDataWithSelect1()
        {
            return null;
        }

        public boolean flush()
        {
            return false;
        }

        public JobSummary waitJobResult(Job job)
                throws ClientException
        {
            return null;
        }

        public Unpacker getJobResult(Job job)
                throws ClientException
        {
            return null;
        }

        public ExtUnpacker getJobResult2(Job job)
                throws ClientException
        {
            return new ExtUnpacker(null, getJobResult(job));
        }

        public void close()
                throws ClientException
        {
        }
    }

    @Test
    public void testTimeout01()
            throws Exception
    {
        { // queryTimeout is not specified. it means queryTimeout = 0.
            int queryTimeout = 0;
            ClientAPI clientApi = new MockClientAPI()
            {
                public JobSummary waitJobResult(Job job)
                        throws ClientException
                {
                    String resultSchema = "[[\"age\",\"int\"],[\"name\",\"string\"]]";
                    return new JobSummary("12345", JobSummary.Type.HIVE,
                            new Database("mugadb"), "url", "rtbl",
                            Status.SUCCESS, "startAt", "endAt", "query",
                            resultSchema);
                }

                public Unpacker getJobResult(Job job)
                        throws ClientException
                {
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
                    }
                    catch (java.io.IOException e) {
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
        { // queryTimeout = 0
            int queryTimeout = 0;
            ClientAPI clientApi = new MockClientAPI()
            {
                public JobSummary waitJobResult(Job job)
                        throws ClientException
                {
                    String resultSchema = "[[\"age\",\"int\"],[\"name\",\"string\"]]";
                    return new JobSummary("12345", JobSummary.Type.HIVE,
                            new Database("mugadb"), "url", "rtbl",
                            Status.SUCCESS, "startAt", "endAt", "query",
                            resultSchema);
                }

                public Unpacker getJobResult(Job job)
                        throws ClientException
                {
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
                    }
                    catch (java.io.IOException e) {
                        throw new ClientException("mock");
                    }
                }
            };
            Job job = new Job("12345");
            ResultSet rs = new TDResultSet(clientApi, 50, job, queryTimeout);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("muga", rs.getString(2));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals("nishizawa", rs.getString(2));
            assertFalse(rs.next());
        }
        { // queryTimeout = 1
            int queryTimeout = 1;
            ClientAPI clientApi = new MockClientAPI()
            {
                public JobSummary waitJobResult(Job job)
                        throws ClientException
                {
                    String resultSchema = "[[\"age\",\"int\"],[\"name\",\"string\"]]";
                    return new JobSummary("12345", JobSummary.Type.HIVE,
                            new Database("mugadb"), "url", "rtbl",
                            Status.SUCCESS, "startAt", "endAt", "query",
                            resultSchema);
                }

                public Unpacker getJobResult(Job job)
                        throws ClientException
                {
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
                    }
                    catch (java.io.IOException e) {
                        throw new ClientException("mock");
                    }
                }
            };
            Job job = new Job("12345");
            ResultSet rs = new TDResultSet(clientApi, 50, job, queryTimeout);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("muga", rs.getString(2));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals("nishizawa", rs.getString(2));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testTimeout02()
            throws Exception
    {
        { // queryTimeout = 1 and queryTimeout is less than sleep time.
            int queryTimeout = 1;
            ClientAPI clientApi = new MockClientAPI()
            {
                public JobSummary waitJobResult(Job job)
                        throws ClientException
                {
                    try {
                        Thread.sleep(3 * 1000);
                    }
                    catch (InterruptedException e) {
                    }

                    String resultSchema = "[[\"age\",\"int\"],[\"name\",\"string\"]]";
                    return new JobSummary("12345", JobSummary.Type.HIVE,
                            new Database("mugadb"), "url", "rtbl",
                            Status.SUCCESS, "startAt", "endAt", "query",
                            resultSchema);
                }

                public Unpacker getJobResult(Job job)
                        throws ClientException
                {
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
                    }
                    catch (java.io.IOException e) {
                        throw new ClientException("mock");
                    }
                }
            };
            Job job = new Job("12345");
            try {
                ResultSet rs = new TDResultSet(clientApi, 50, job, queryTimeout);
                rs.next();
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof SQLException); // it is thrown by
                // fetchRows
                assertTrue(t.getCause() instanceof TimeoutException);
            }
        }
        { // queryTimeout = 3 and queryTimeout is not less than sleep time.
            int queryTimeout = 3;
            ClientAPI clientApi = new MockClientAPI()
            {
                public JobSummary waitJobResult(Job job)
                        throws ClientException
                {
                    try {
                        Thread.sleep(1 * 1000);
                    }
                    catch (InterruptedException e) {
                    }

                    String resultSchema = "[[\"age\",\"int\"],[\"name\",\"string\"]]";
                    return new JobSummary("12345", JobSummary.Type.HIVE,
                            new Database("mugadb"), "url", "rtbl",
                            Status.SUCCESS, "startAt", "endAt", "query",
                            resultSchema);
                }

                public Unpacker getJobResult(Job job)
                        throws ClientException
                {
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
                    }
                    catch (java.io.IOException e) {
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
    }

    @Test
    public void testNext01()
            throws Exception
    {
        ClientAPI clientApi = new MockClientAPI()
        {
            public JobSummary waitJobResult(Job job)
                    throws ClientException
            {
                String resultSchema = "[[\"age\",\"int\"],[\"name\",\"string\"]]";
                return new JobSummary("12345", JobSummary.Type.HIVE,
                        new Database("mugadb"), "url", "rtbl", Status.SUCCESS,
                        "startAt", "endAt", "query", resultSchema);
            }

            public Unpacker getJobResult(Job job)
                    throws ClientException
            {
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
                }
                catch (java.io.IOException e) {
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
    public void testNext02()
            throws Exception
    {
        ClientAPI clientApi = new MockClientAPI()
        {
            public JobSummary waitJobResult(Job job)
                    throws ClientException
            {
                String resultSchema = "[[\"age\",\"int\"],[\"name\",\"string\"]]";
                return new JobSummary("12345", JobSummary.Type.HIVE,
                        new Database("mugadb"), "url", "rtbl", Status.SUCCESS,
                        "startAt", "endAt", "query", resultSchema);
            }

            public Unpacker getJobResult(Job job)
                    throws ClientException
            {
                try {
                    MessagePack msgpack = new MessagePack();
                    BufferPacker packer = msgpack.createBufferPacker();
                    List<Object> ret0 = new ArrayList<Object>();
                    ret0.add(10);
                    ret0.add("muga");
                    packer.write(ret0);
                    byte[] bytes = packer.toByteArray();
                    return msgpack.createBufferUnpacker(bytes);
                }
                catch (java.io.IOException e) {
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
        { // ok: int to string
            assertEquals("10", rs.getString(1));
        }
        { // ok: int to boolean
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    public void testNext03()
            throws Exception
    {
        final int count = 100;
        ClientAPI clientApi = new MockClientAPI()
        {
            public JobSummary waitJobResult(Job job)
                    throws ClientException
            {
                String resultSchema = "[[\"p1\",\"string\"],[\"p2\",\"string\"]]";
                return new JobSummary("12345", JobSummary.Type.HIVE,
                        new Database("mugadb"), "url", "rtbl", Status.SUCCESS,
                        "startAt", "endAt", "query", resultSchema);
            }

            public Unpacker getJobResult(Job job)
                    throws ClientException
            {
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
                }
                catch (java.io.IOException e) {
                    throw new ClientException("mock");
                }
            }
        };

        Job job = new Job("12345");
        ResultSet rs = new TDResultSet(clientApi, 100, job);
        for (int i = 0; i < count; i++) {
            assertTrue(rs.next());
            assertEquals("p1:" + i, rs.getString(1));
            assertEquals("p2:" + i, rs.getString(2));
        }
        assertFalse(rs.next());
    }

    @Test
    public void testGetMetaData01()
            throws Exception
    {
        ClientAPI clientApi = new MockClientAPI()
        {
            public JobSummary waitJobResult(Job job)
                    throws ClientException
            {
                String resultSchema = "[[\"p1\",\"string\"],[\"p2\",\"float\"],[\"p3\",\"double\"],[\"p4\",\"boolean\"]]";
                return new JobSummary("12345", JobSummary.Type.HIVE,
                        new Database("mugadb"), "url", "rtbl", Status.SUCCESS,
                        "startAt", "endAt", "query", resultSchema);
            }
        };
        Job job = new Job("12345");
        TDResultSet rs = new TDResultSet(clientApi, 50, job);
        ResultSetMetaData rsMetaData = rs.getMetaData();
        { // getColumnType(int)
            try {
                rsMetaData.getColumnType(0);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            assertEquals(Utils.TDTypeToSqlType(Constants.STRING_TYPE_NAME),
                    rsMetaData.getColumnType(1));
            assertEquals(Utils.TDTypeToSqlType(Constants.FLOAT_TYPE_NAME),
                    rsMetaData.getColumnType(2));
            assertEquals(Utils.TDTypeToSqlType(Constants.DOUBLE_TYPE_NAME),
                    rsMetaData.getColumnType(3));
            assertEquals(Utils.TDTypeToSqlType(Constants.BOOLEAN_TYPE_NAME),
                    rsMetaData.getColumnType(4));
            try {
                rsMetaData.getColumnType(5);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
        }
        { // getColumnTypeName(int)
            try {
                rsMetaData.getColumnTypeName(0);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            assertEquals(Constants.STRING_TYPE_NAME,
                    rsMetaData.getColumnTypeName(1));
            assertEquals(Constants.FLOAT_TYPE_NAME,
                    rsMetaData.getColumnTypeName(2));
            assertEquals(Constants.DOUBLE_TYPE_NAME,
                    rsMetaData.getColumnTypeName(3));
            assertEquals(Constants.BOOLEAN_TYPE_NAME,
                    rsMetaData.getColumnTypeName(4));
            try {
                rsMetaData.getColumnTypeName(5);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
        }
        { // getColumnName(int)
            try {
                rsMetaData.getColumnName(0);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof ArrayIndexOutOfBoundsException);
            }
            assertEquals("p1", rsMetaData.getColumnName(1));
            assertEquals("p2", rsMetaData.getColumnName(2));
            assertEquals("p3", rsMetaData.getColumnName(3));
            assertEquals("p4", rsMetaData.getColumnName(4));
            try {
                rsMetaData.getColumnName(5);
            }
            catch (Throwable t) {
                assertTrue(t instanceof IndexOutOfBoundsException);
            }
        }
    }

    @Test
    public void testGetMetaData02()
            throws Exception
    {
        ClientAPI clientApi = new MockClientAPI()
        {
            public JobSummary waitJobResult(Job job)
                    throws ClientException
            {
                String resultSchema = "[[\"p1\",\"tinyint\"],[\"p2\",\"smallint\"],[\"p3\",\"int\"],[\"p4\",\"bigint\"]]";
                return new JobSummary("12345", JobSummary.Type.HIVE,
                        new Database("mugadb"), "url", "rtbl", Status.SUCCESS,
                        "startAt", "endAt", "query", resultSchema);
            }
        };
        Job job = new Job("12345");
        TDResultSet rs = new TDResultSet(clientApi, 50, job);
        ResultSetMetaData rsMetaData = rs.getMetaData();
        { // getColumnType(int)
            try {
                rsMetaData.getColumnType(0);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            assertEquals(Utils.TDTypeToSqlType(Constants.TINYINT_TYPE_NAME),
                    rsMetaData.getColumnType(1));
            assertEquals(Utils.TDTypeToSqlType(Constants.SMALLINT_TYPE_NAME),
                    rsMetaData.getColumnType(2));
            assertEquals(Utils.TDTypeToSqlType(Constants.INT_TYPE_NAME),
                    rsMetaData.getColumnType(3));
            assertEquals(Utils.TDTypeToSqlType(Constants.BIGINT_TYPE_NAME),
                    rsMetaData.getColumnType(4));
            try {
                rsMetaData.getColumnType(5);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
        }
        { // getColumnTypeName(int)
            try {
                rsMetaData.getColumnTypeName(0);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            assertEquals(Constants.TINYINT_TYPE_NAME,
                    rsMetaData.getColumnTypeName(1));
            assertEquals(Constants.SMALLINT_TYPE_NAME,
                    rsMetaData.getColumnTypeName(2));
            assertEquals(Constants.INT_TYPE_NAME,
                    rsMetaData.getColumnTypeName(3));
            assertEquals(Constants.BIGINT_TYPE_NAME,
                    rsMetaData.getColumnTypeName(4));
            try {
                rsMetaData.getColumnTypeName(5);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
        }
        { // getColumnName(int)
            try {
                rsMetaData.getColumnName(0);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof ArrayIndexOutOfBoundsException);
            }
            assertEquals("p1", rsMetaData.getColumnName(1));
            assertEquals("p2", rsMetaData.getColumnName(2));
            assertEquals("p3", rsMetaData.getColumnName(3));
            assertEquals("p4", rsMetaData.getColumnName(4));
            try {
                rsMetaData.getColumnName(5);
            }
            catch (Throwable t) {
                assertTrue(t instanceof IndexOutOfBoundsException);
            }
        }
    }

    @Test
    public void testGetMetaData03()
            throws Exception
    {
        ClientAPI clientApi = new MockClientAPI()
        {
            public JobSummary waitJobResult(Job job)
                    throws ClientException
            {
                String resultSchema = "[[\"p1\",\"map<string,int>\"],[\"p2\",\"array<int>\"],[\"p3\",\"struct<int>\"]]";
                return new JobSummary("12345", JobSummary.Type.HIVE,
                        new Database("mugadb"), "url", "rtbl", Status.SUCCESS,
                        "startAt", "endAt", "query", resultSchema);
            }
        };
        Job job = new Job("12345");
        TDResultSet rs = new TDResultSet(clientApi, 50, job);
        ResultSetMetaData rsMetaData = rs.getMetaData();
        { // getColumnType(int)
            try {
                rsMetaData.getColumnType(0);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            assertEquals(Utils.TDTypeToSqlType(Constants.STRING_TYPE_NAME),
                    rsMetaData.getColumnType(1));
            assertEquals(Utils.TDTypeToSqlType(Constants.STRING_TYPE_NAME),
                    rsMetaData.getColumnType(2));
            assertEquals(Utils.TDTypeToSqlType(Constants.STRING_TYPE_NAME),
                    rsMetaData.getColumnType(3));
            try {
                rsMetaData.getColumnType(4);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
        }
        { // getColumnTypeName(int)
            try {
                rsMetaData.getColumnTypeName(0);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
            assertEquals(Constants.STRING_TYPE_NAME,
                    rsMetaData.getColumnTypeName(1));
            assertEquals(Constants.STRING_TYPE_NAME,
                    rsMetaData.getColumnTypeName(2));
            assertEquals(Constants.STRING_TYPE_NAME,
                    rsMetaData.getColumnTypeName(3));
            try {
                rsMetaData.getColumnTypeName(4);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
        }
        { // getColumnName(int)
            try {
                rsMetaData.getColumnName(0);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof ArrayIndexOutOfBoundsException);
            }
            assertEquals("p1", rsMetaData.getColumnName(1));
            assertEquals("p2", rsMetaData.getColumnName(2));
            assertEquals("p3", rsMetaData.getColumnName(3));
            try {
                rsMetaData.getColumnName(4);
            }
            catch (Throwable t) {
                assertTrue(t instanceof IndexOutOfBoundsException);
            }
        }
    }
}
