package com.treasure_data.jdbc;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.littleshoot.proxy.HttpProxyServer;
import org.littleshoot.proxy.ProxyAuthenticator;
import org.littleshoot.proxy.impl.DefaultHttpProxyServer;

import java.io.IOException;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class TestProxy
{
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


}
