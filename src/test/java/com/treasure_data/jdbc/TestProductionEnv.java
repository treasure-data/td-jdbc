package com.treasure_data.jdbc;

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
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

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
            logger.warn("config file %s is not found", file);
            return p;
        }

        BufferedReader reader = new BufferedReader(new FileReader(file));
        StringBuilder extracted = new StringBuilder();
        String line = null;
        while((line = reader.readLine()) != null) {
            if(line.trim().startsWith("[")) {
                continue; // skip [... ] line
            }
            extracted.append(line.trim());
            extracted.append("\n");
        }
        String props = extracted.toString();
        p.load(new StringReader(props));
        return p;
    }

    public static Connection newPrestoConnection(String database)
            throws IOException, SQLException
    {
        Properties prop = readTDConf();
        Connection conn = DriverManager.getConnection(
                String.format("jdbc:td://api.treasuredata.com/%s;useSSL=true;type=presto", database),
                prop.getProperty("user", ""),
                prop.getProperty("password", "")
        );
        return conn;
    }


    @Ignore
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
            if (rs.next()) {
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
            }
            assertFalse(rs.next());
            rs.close();
        }

        stat.execute("select nums from arraytest");
        {
            ResultSet rs = stat.getResultSet();
            if(rs.next()) {
                ArrayValue arr = (ArrayValue) rs.getObject(1);
                logger.debug("getObject result: {}, type: {}", arr, arr.getClass());
                for (int i = 0; i < arr.size(); ++i) {
                    int v = arr.get(i).asIntegerValue().getInt();
                    assertEquals(i + 1, v);
                }
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
        if(rs.next()) {
            ArrayValue arr = (ArrayValue) rs.getObject(1);
            logger.debug("getObject result: {}, type: {}", arr, arr.getClass());
            for (int i = 0; i < arr.size(); ++i) {
                int v = arr.get(i).asIntegerValue().getInt();
                assertEquals(i + 1, v);
            }
        }
        assertFalse(rs.next());
        rs.close();
        stat.close();
        conn.close();

    }

}
