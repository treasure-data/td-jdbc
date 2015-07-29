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

import org.junit.Ignore;
import org.junit.Test;
import org.msgpack.type.ArrayValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test cases for integration testing with production API
 */

public class TestProductionEnv
{
    private static Logger logger = LoggerFactory.getLogger(TestProductionEnv.class);

    /**
     * Read user user (e-mail address and password properties from $HOME/.td/td.conf
     * @return
     * @throws IOException
     */
    public static Properties readTDConf()
            throws IOException
    {
        Properties p = new Properties();
        File file = new File(System.getProperty("user.home", "./"), String.format(".td/td.conf"));
        if(!file.exists()) {
            logger.warn(String.format("config file %s is not found", file));
            return p;
        }

        BufferedReader reader = new BufferedReader(new FileReader(file));
        StringBuilder extracted = new StringBuilder();
        String line = null;
        while((line = reader.readLine()) != null) {
            String trimmed = line.trim();
            if(trimmed.startsWith("[") || trimmed.startsWith("#")) {
                continue; // skip [... ] line or comment line
            }
            extracted.append(line.trim());
            extracted.append("\n");
        }
        String props = extracted.toString();
        p.load(new StringReader(props));
        return p;
    }

    private static String firstNonNull(Object... keys) {
        if(keys != null) {
            for (Object k : keys) {
                if (k != null) {
                    return k.toString();
                }
            }
        }
        return "";
    }

    public static Connection newConnection(String jdbcUrl, Properties config)
            throws SQLException, IOException
    {
        Properties prop = readTDConf();
        Map<String, String> env = System.getenv();
        Properties connectionProp = new Properties();
        connectionProp.putAll(config);
        connectionProp.setProperty("user", firstNonNull(config.getProperty("user"), prop.get("user"), env.get("TD_USER")));
        connectionProp.setProperty("password", firstNonNull(config.getProperty("password"), prop.get("password"), env.get("TD_PASS")));
        connectionProp.setProperty("apikey", firstNonNull(config.getProperty("apikey"), prop.get("apikey")));
        Connection conn = DriverManager.getConnection(jdbcUrl, connectionProp);
        return conn;
    }

    public static Connection newPrestoConnection(String database)
            throws IOException, SQLException
    {
        return newPrestoConnection(database, new Properties());
    }

    public static Connection newPrestoConnection(String database, Properties config)
            throws IOException, SQLException
    {
        return newConnection(String.format("jdbc:td://api.treasuredata.com/%s;useSSL=true;type=presto", database), config);
    }

    @Test
    public void testNonSSLConnection()
            throws IOException, SQLException
    {
        Connection conn = newConnection("jdbc:td://api.treasuredata.com/leodb", new Properties());
        Statement stat = conn.createStatement();
        stat.execute("select 1 + 1");
        ResultSet rs = stat.getResultSet();
        assertTrue(rs.next());
        int result = rs.getInt(1);
        assertEquals(2, result);
        assertFalse(rs.next());
        rs.close();
        stat.close();
        conn.close();
    }

    @Test
    public void readArrayType()
            throws SQLException, IOException
    {
        /**
         * <pre>
            presto:cs_modeanalytics> select * from arraytest;
            id |   nums    |    time
            ----+-----------+------------
            1 | [1, 2, 3] | 1432099776
            (1 row)
         </pre>
         */

        Connection conn = newPrestoConnection("cs_modeanalytics");
        Statement stat = conn.createStatement();
        stat.execute("select time, id, nums from arraytest");
        {
            ResultSet rs = stat.getResultSet();
            assertTrue(rs.next());
            long time = rs.getLong(1);
            assertEquals(1432099776L, time);
            int id = rs.getInt(2);
            assertEquals(1, id);

            ArrayValue arr = (ArrayValue) rs.getObject(3);
            logger.debug("getObject result: {}, type: {}", arr, arr.getClass());
            for (int i = 0; i < arr.size(); ++i) {
                int v = arr.get(i).asIntegerValue().getInt();
                assertEquals(i + 1, v);
            }
            assertFalse(rs.next());
            rs.close();
        }

        stat.execute("select nums from arraytest");
        {
            ResultSet rs = stat.getResultSet();
            assertTrue(rs.next());
            ArrayValue arr = (ArrayValue) rs.getObject(1);
            logger.debug("getObject result: {}, type: {}", arr, arr.getClass());
            for (int i = 0; i < arr.size(); ++i) {
                int v = arr.get(i).asIntegerValue().getInt();
                assertEquals(i + 1, v);
            }
            assertFalse(rs.next());
            rs.close();
        }

        stat.close();
        conn.close();
    }

    @Ignore
    @Test
    public void readJsonArray()
            throws IOException, SQLException
    {
        Connection conn = newPrestoConnection("cs_modeanalytics");
        Statement stat = conn.createStatement();

        // nums = '[1, 2, 3]' (varchar) but its data type is changed to array<int> from TD console
        stat.execute("select nums from arraytest_str");
        ResultSet rs = stat.getResultSet();
        assertTrue(rs.next());
        ArrayValue arr = (ArrayValue) rs.getObject(1);
        logger.debug("getObject result: {}, type: {}", arr, arr.getClass());
        for (int i = 0; i < arr.size(); ++i) {
            int v = arr.get(i).asIntegerValue().getInt();
            assertEquals(i + 1, v);
        }
        assertFalse(rs.next());
        rs.close();
        stat.close();
        conn.close();

    }

    @Test
    public void select1()
            throws IOException, SQLException
    {
        Connection conn = newPrestoConnection("leodb");
        Statement stat = conn.createStatement();
        stat.execute("select 1");
        ResultSet rs = stat.getResultSet();
        logger.debug("rs class: " + rs.getClass());
        assertTrue(rs.next());
        int one = rs.getInt(1);
        assertEquals(1, one);

        assertFalse(rs.next());
        rs.close();
        stat.close();
        conn.close();
    }

    @Test
    public void testErrorMessage()
            throws IOException, SQLException
    {
        try {
            Connection conn = newPrestoConnection("leodb");
            Statement stat = conn.createStatement();
            boolean ret = stat.execute("select * from unknown_table"); // incomplete statement
            ResultSet rs = stat.getResultSet();
            assertFalse(rs.next());
            rs.close();
            stat.close();
            conn.close();

            fail("Cannot reach here");
        }
        catch(Exception e) {
            e.printStackTrace();
            String msg = e.getMessage();
            logger.warn("--error message:{}--", msg);
            assertTrue("SQLException should report missing table", msg.toLowerCase().contains("unknown_table does not exist"));
        }

    }


    @Test
    public void testPerformance()
            throws IOException, SQLException
    {
        Connection conn = newPrestoConnection("leodb");
        for(int i=0; i<3; ++i) {
            long started = System.currentTimeMillis();
            try {
                Statement stat = conn.createStatement();
                boolean ret = stat.execute("select source_ip, dest_url, visit_date, ad_revenue, country_code from hivebench_tiny.uservisits limit 1000"); // incomplete statement
                //boolean ret = stat.execute("select 2"); // incomplete statement
                ResultSet rs = stat.getResultSet();
                int count = 0;
                while(rs.next()) {
                    rs.getString(1);
                    rs.getString(2);
                    rs.getString(3);
                    rs.getDouble(4);
                    rs.getString(5);
                    count++;
                }
                assertEquals(1000, count);
                rs.close();
                stat.close();
            }
            catch(Exception e) {
                logger.error("failed", e);
            }
            long finished = System.currentTimeMillis();
            logger.info(String.format("------------------------- %.2f sec.", (finished - started) / 1000.0));
        }
        conn.close();
    }


}
