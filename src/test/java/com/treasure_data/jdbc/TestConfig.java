package com.treasure_data.jdbc;

import org.junit.Test;

import java.sql.SQLException;

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
        assertEquals(80, config.apiConfig.port);
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
        assertEquals(80, config.apiConfig.port);
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
            assertEquals(80, config.apiConfig.port);
            assertEquals("db01", config.database);
            assertEquals(null, config.user);
            assertEquals(null, config.password);
        }
    }
}