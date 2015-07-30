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

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.littleshoot.proxy.HttpFilters;
import org.littleshoot.proxy.HttpFiltersSourceAdapter;
import org.littleshoot.proxy.HttpProxyServer;
import org.littleshoot.proxy.ProxyAuthenticator;
import org.littleshoot.proxy.impl.DefaultHttpProxyServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 *
 */
public class TestProxy
{
    private static Logger logger = LoggerFactory.getLogger(TestProxy.class);

    private HttpProxyServer proxyServer;
    private int proxyPort;

    private static int findAvailablePort()
            throws IOException
    {
        ServerSocket socket = new ServerSocket(0);
        try {
            int port = socket.getLocalPort();
            return port;
        }
        finally {
            socket.close();
        }
    }

    private static final String PROXY_USER = "test";
    private static final String PROXY_PASS = "helloproxy";

    private AtomicInteger proxyAccessCount = new AtomicInteger(0);

    @Before
    public void setUp() throws Exception {
        proxyAccessCount.set(0);
        this.proxyPort = findAvailablePort();
        this.proxyServer = DefaultHttpProxyServer.bootstrap().withPort(proxyPort).withProxyAuthenticator(new ProxyAuthenticator()
        {
            @Override
            public boolean authenticate(String user, String pass)
            {
                return user.equals(PROXY_USER) && pass.equals(PROXY_PASS);
            }
        }).withFiltersSource(new HttpFiltersSourceAdapter()
        {
            @Override
            public HttpFilters filterRequest(HttpRequest httpRequest, ChannelHandlerContext channelHandlerContext)
            {
                proxyAccessCount.incrementAndGet();
                return super.filterRequest(httpRequest, channelHandlerContext);
            }
        }).start();

        // Unset proxy configuration
        System.clearProperty("http.proxyHost");
        System.clearProperty("https.proxyHost");
        System.clearProperty("http.proxyPort");
        System.clearProperty("https.proxyPort");
        System.clearProperty("http.proxyUser");
        System.clearProperty("http.proxyPassword");
    }

    @After
    public void tearDown() throws Exception {
        if(this.proxyServer != null) {
            proxyServer.stop();
        }
    }

    @Test
    public void connectThroughProxy()
            throws IOException, SQLException
    {
        Connection conn = TestProductionEnv.newConnection(String.format(
                "jdbc:td://api.treasuredata.com/sample_datasets;useSSL=true;type=presto;httpproxyhost=localhost;httpproxyport=%d;httpproxyuser=%s;httpproxypassword=%s",
                proxyPort,
                PROXY_USER,
                PROXY_PASS),
                new Properties()
        );
        Statement stat = conn.createStatement();
        stat.execute("select count(*) from www_access");
        ResultSet rs = stat.getResultSet();
        assertTrue(rs.next());
        int rowCount = rs.getInt(1);
        assertEquals(5000, rowCount);
        stat.close();
        conn.close();

        assertTrue("no proxy access", proxyAccessCount.get() > 0);
    }

    private static void assertFunction(Properties prop, String sqlProjection, String expectedAnswer)
            throws IOException, SQLException
    {
        Connection conn = TestProductionEnv.newPrestoConnection("sample_datasets", prop);
        Statement stat = conn.createStatement();
        String sql = String.format("select %s from www_access", sqlProjection);
        stat.execute(sql);
        ResultSet rs = stat.getResultSet();
        assert(rs.next());
        String col = rs.getString(1);
        logger.debug("result: " + col);
        rs.close();
        stat.close();
        conn.close();
    }


    private Properties getJdbcProxyConfig() {
        Properties prop = new Properties();
        prop.setProperty("httpproxyhost", "localhost");
        prop.setProperty("httpproxyport", Integer.toString(proxyPort));
        prop.setProperty("httpproxyuser", PROXY_USER);
        prop.setProperty("httpproxypassword", PROXY_PASS);
        return prop;
    }

    @Test
    public void proxyConfigViaProperties()
            throws IOException, SQLException
    {
        assertFunction(getJdbcProxyConfig(), "count(*)", "10000");
        assertTrue("no proxy access", proxyAccessCount.get() > 0);
    }

    @Test
    public void proxyConfigViaSystemProperties()
            throws IOException, SQLException
    {
        System.setProperty("http.proxyHost", "localhost");
        System.setProperty("http.proxyPort", Integer.toString(proxyPort));
        System.setProperty("http.proxyUser", PROXY_USER);
        System.setProperty("http.proxyPassword", PROXY_PASS);

        Properties emptyProp = new Properties();
        assertFunction(emptyProp, "count(*)", "5000");
        assertTrue("no proxy access", proxyAccessCount.get() > 0);
    }

    @Test
    public void detectWrongProxyPassword()
            throws IOException, SQLException
    {
        Properties prop = getJdbcProxyConfig();
        prop.setProperty("httpproxyuser", "testtest"); // set a wrong password
        System.setProperty("td.client.retry.count", "2"); // For workaround of #1
        Statement stat;
        try {
            Connection conn = TestProductionEnv.newPrestoConnection("sample_datasets", prop);
            stat = conn.createStatement();
            stat.execute("select count(*) from www_access");
        }
        catch(SQLException e) {
            // Should display authentication failure message here
            logger.error("authentication failure", e);
            return;
        }
        ResultSet rs = stat.getResultSet();
        rs.next();
        int result = rs.getInt(1);
        logger.debug("result: " + result);
        fail("should not reach here");
    }
}
