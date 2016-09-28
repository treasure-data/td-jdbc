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

import org.junit.Test;

import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestConfig
{
    @Test
    public void testFull()
            throws Exception
    {
        final String url = "jdbc:td://host01:9999/db01;user=user01;password=pass01;k01=v01;k02=v02";
        Config config = Config.parseJdbcURL(url);
        assertEquals(url, config.url);
        assertEquals("host01", config.apiConfig.endpoint);
        assertEquals(9999, config.apiConfig.port);
        assertEquals("db01", config.database);
        assertEquals("user01", config.user);
        assertEquals("pass01", config.password);
    }

    @Test
    public void testType()
            throws Exception
    {
        final String url = "jdbc:td://";
        try {
            Config.parseJdbcURL(url);
            fail();
        }
        catch (Throwable t) {
            assertTrue(t instanceof SQLException);
        }
    }

    @Test
    public void testInvalidType()
            throws Exception
    {
        final String url = "odbc:td://host01:9999/db01;user=user01;password=pass01;k01=v01;k02=v02";
        try {
            Config.parseJdbcURL(url);
            fail();
        }
        catch (Throwable t) {
            assertTrue(t instanceof SQLException);
        }
    }

    @Test
    public void testNoHost()
            throws Exception
    {
        final String url = "jdbc:td://:9999/db01;user=user01;password=pass01;k01=v01;k02=v02";
        Config config = Config.parseJdbcURL(url);
        assertEquals(url, config.url);
        assertEquals("api.treasuredata.com", config.apiConfig.endpoint);
        assertEquals(9999, config.apiConfig.port);
        assertEquals("db01", config.database);
        assertEquals("user01", config.user);
        assertEquals("pass01", config.password);
    }

    @Test
    public void testNoPort()
            throws Exception
    {
        final String url = "jdbc:td://host01/db01;user=user01;password=pass01;k01=v01;k02=v02";
        Config config = Config.parseJdbcURL(url);
        assertEquals(url, config.url);
        assertEquals("host01", config.apiConfig.endpoint);
        assertEquals(443, config.apiConfig.port);
        assertEquals("db01", config.database);
        assertEquals("user01", config.user);
        assertEquals("pass01", config.password);
    }

    @Test
    public void testInvalidPort()
            throws Exception
    {
        {
            final String url = "jdbc:td://host01:/db01;user=user01;password=pass01;k01=v01;k02=v02";
            try {
                Config.parseJdbcURL(url);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
        }
        {
            final String url = "jdbc:td://host01:str/db01;user=user01;password=pass01;k01=v01;k02=v02";
            try {
                Config.parseJdbcURL(url);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
        }
    }

    @Test
    public void testNoHostAndPort()
            throws Exception
    {
        final String url = "jdbc:td:///db01;user=user01;password=pass01;k01=v01;k02=v02";
        Config config = Config.parseJdbcURL(url);
        assertEquals(url, config.url);
        assertEquals("api.treasuredata.com", config.apiConfig.endpoint);
        assertEquals(443, config.apiConfig.port);
        assertEquals("db01", config.database);
        assertEquals("user01", config.user);
        assertEquals("pass01", config.password);
    }

    @Test
    public void testNoDatabase()
            throws Exception
    {
        {
            final String url = "jdbc:td://host01;user=user01;password=pass01;k01=v01;k02=v02";
            Config config = Config.parseJdbcURL(url);
            assertEquals("default", config.database);
        }
        {
            final String url = "jdbc:td://host01/;user=user01;password=pass01;k01=v01;k02=v02";
            Config config = Config.parseJdbcURL(url);
            assertEquals("default", config.database);
        }
    }

    @Test
    public void testNoParameters()
            throws Exception
    {
        {
            final String url = "jdbc:td://host01:9999/db01";
            Config config = Config.parseJdbcURL(url);
            assertEquals(url, config.url);
            assertEquals("host01", config.apiConfig.endpoint);
            assertEquals(9999, config.apiConfig.port);
            assertEquals("db01", config.database);
            assertEquals(null, config.user);
            assertEquals(null, config.password);
        }
        {
            final String url = "jdbc:td:///db01";
            Config config = Config.parseJdbcURL(url);
            assertEquals(url, config.url);
            assertEquals("api.treasuredata.com", config.apiConfig.endpoint);
            assertEquals(443, config.apiConfig.port);
            assertEquals("db01", config.database);
            assertEquals(null, config.user);
            assertEquals(null, config.password);
        }
    }

    @Test
    public void testEmptyParameters()
            throws Exception
    {
        Properties props = new Properties();
        props.putAll(System.getProperties());

        props.setProperty("user",  "user01");
        props.setProperty("password",  "pass01");
        props.setProperty("td.jdbc.result.retrycount.threshold", "");
        props.setProperty("td.jdbc.result.retry.waittime", "");
        props.setProperty("usessl", "");
        props.setProperty("httpproxyhost", "");
        props.setProperty("httpproxyport", "");
        props.setProperty("httpproxyuser", "");
        props.setProperty("httpproxypassword", "");

        String url = "jdbc:td://host01:9999/db01";
        Config config = Config.newConfig(url, props);
        assertEquals(url, config.url);
        assertEquals("host01", config.apiConfig.endpoint);
        assertEquals(9999, config.apiConfig.port);
        assertEquals("db01", config.database);
        assertEquals("user01", config.user);
        assertEquals("pass01", config.password);
    }
}