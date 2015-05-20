package com.treasure_data.jdbc;

import org.junit.Test;

import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestJDBCURLParser
{
    @Test
    public void testFull()
            throws Exception
    {
        final String url = "jdbc:td://host01:9999/db01;user=user01;password=pass01;k01=v01;k02=v02";
        JDBCURLParser.Desc d = JDBCURLParser.parse(url);
        assertEquals(url, d.url);
        assertEquals("host01", d.host);
        assertEquals("9999", d.port);
        assertEquals("db01", d.database);
        assertEquals("user01", d.user);
        assertEquals("pass01", d.password);
    }

    @Test
    public void testType()
            throws Exception
    {
        final String url = "jdbc:td://";
        try {
            JDBCURLParser.parse(url);
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
            JDBCURLParser.parse(url);
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
        JDBCURLParser.Desc d = JDBCURLParser.parse(url);
        assertEquals(url, d.url);
        assertEquals("api.treasuredata.com", d.host);
        assertEquals("9999", d.port);
        assertEquals("db01", d.database);
        assertEquals("user01", d.user);
        assertEquals("pass01", d.password);
    }

    @Test
    public void testNoPort()
            throws Exception
    {
        final String url = "jdbc:td://host01/db01;user=user01;password=pass01;k01=v01;k02=v02";
        JDBCURLParser.Desc d = JDBCURLParser.parse(url);
        assertEquals(url, d.url);
        assertEquals("host01", d.host);
        assertEquals(null, d.port);
        assertEquals("db01", d.database);
        assertEquals("user01", d.user);
        assertEquals("pass01", d.password);
    }

    @Test
    public void testInvalidPort()
            throws Exception
    {
        {
            final String url = "jdbc:td://host01:/db01;user=user01;password=pass01;k01=v01;k02=v02";
            try {
                JDBCURLParser.parse(url);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
        }
        {
            final String url = "jdbc:td://host01:str/db01;user=user01;password=pass01;k01=v01;k02=v02";
            try {
                JDBCURLParser.parse(url);
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
        JDBCURLParser.Desc d = JDBCURLParser.parse(url);
        assertEquals(url, d.url);
        assertEquals("api.treasuredata.com", d.host);
        assertEquals(null, d.port);
        assertEquals("db01", d.database);
        assertEquals("user01", d.user);
        assertEquals("pass01", d.password);
    }

    @Test
    public void testNoDatabase()
            throws Exception
    {
        {
            final String url = "jdbc:td://host01;user=user01;password=pass01;k01=v01;k02=v02";
            try {
                JDBCURLParser.parse(url);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
        }
        {
            final String url = "jdbc:td://host01/;user=user01;password=pass01;k01=v01;k02=v02";
            try {
                JDBCURLParser.parse(url);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }
        }
    }

    @Test
    public void testNoParameters()
            throws Exception
    {
        {
            final String url = "jdbc:td://host01:9999/db01";
            JDBCURLParser.Desc d = JDBCURLParser.parse(url);
            assertEquals(url, d.url);
            assertEquals("host01", d.host);
            assertEquals("9999", d.port);
            assertEquals("db01", d.database);
            assertEquals(null, d.user);
            assertEquals(null, d.password);
        }
        {
            final String url = "jdbc:td:///db01";
            JDBCURLParser.Desc d = JDBCURLParser.parse(url);
            assertEquals(url, d.url);
            assertEquals("api.treasuredata.com", d.host);
            assertEquals(null, d.port);
            assertEquals("db01", d.database);
            assertEquals(null, d.user);
            assertEquals(null, d.password);
        }
    }
}