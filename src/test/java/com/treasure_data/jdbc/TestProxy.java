package com.treasure_data.jdbc;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
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

    @Before
    public void setUp() throws Exception {

        this.proxyPort = findAvailablePort();
        this.proxyServer = DefaultHttpProxyServer.bootstrap().withPort(proxyPort).withProxyAuthenticator(new ProxyAuthenticator() {
            @Override
            public boolean authenticate(String user, String pass)
            {
                return user.equals("test") && pass.equals("helloproxy");
            }
        }).start();
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
                "jdbc:td://api.treasuredata.com/hivebench_tiny;useSSL=true;type=presto;httpproxyhost=localhost;httpproxyport=%d;httpproxyuser=%s;httpproxypassword=%s",
                proxyPort,
                "test",
                "helloproxy"),
                new Properties()
        );
        Statement stat = conn.createStatement();
        stat.execute("select count(*) from hivebench_tiny.uservisits");
        ResultSet rs = stat.getResultSet();
        assertTrue(rs.next());
        int rowCount = rs.getInt(1);
        assertEquals(10000, rowCount);
        stat.close();
        conn.close();
    }


    private static void assertFunction(Properties prop, String sqlProjection, String expectedAnswer)
            throws IOException, SQLException
    {

        Connection conn = TestProductionEnv.newPrestoConnection("hivebench_tiny", prop);
        Statement stat = conn.createStatement();
        String sql = String.format("select %s from uservisits", sqlProjection);
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
        prop.setProperty("httpproxyuser", "test");
        prop.setProperty("httpproxypassword", "helloproxy");
        return prop;
    }

    @Test
    public void proxyConfigViaProperties()
            throws IOException, SQLException
    {
        assertFunction(getJdbcProxyConfig(), "count(*)", "10000");
    }

    @Test
    public void proxyConfigViaSystemProperties()
            throws IOException, SQLException
    {
        Properties prop = new Properties();
        String prevProxyHost = System.setProperty("http.proxyHost", "localhost");
        String prevProxyPort = System.setProperty("http.proxyPort", Integer.toString(proxyPort));
        String prevProxyUser = System.setProperty("http.proxyUser", "test");
        String prevProxyPass = System.setProperty("http.proxyPassword", "helloproxy");

        try {
            assertFunction(prop, "count(*)", "10000");
        }
        finally {
            if(prevProxyHost != null) {
                System.setProperty("http.proxyHost", prevProxyHost);
            }
            if(prevProxyPort != null) {
                System.setProperty("http.proxyPort", prevProxyPort);
            }
            if(prevProxyUser != null) {
                System.setProperty("http.proxyUser", prevProxyUser);
            }
            if(prevProxyPass != null) {
                System.setProperty("http.proxyPassword", prevProxyPass);
            }
        }
    }


    @Ignore
    @Test
    public void detectWrongProxyPassword()
            throws IOException, SQLException
    {
        Properties prop = getJdbcProxyConfig();
        prop.setProperty("httpproxyuser", "testtest"); // set wrong password

        Connection conn = TestProductionEnv.newPrestoConnection("hivebench_tiny", prop);
        Statement stat = conn.createStatement();
        try {
            stat.execute("select count(*) from uservisits");
        }
        catch(SQLException e) {
            // Should display authentication failure message here
            logger.error("authentication failure", e);
            return;
        }
        ResultSet rs = stat.getResultSet();
        String result = rs.getString(1);
        logger.debug("result: " + result);
        fail("should not reach here");
    }



}
