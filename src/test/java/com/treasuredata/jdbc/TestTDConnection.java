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

import com.mysql.jdbc.exceptions.jdbc4.CommunicationsException;
import junit.framework.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class TestTDConnection
{
    @Test
    @Ignore
    public void testSimple()
            throws Exception
    {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));

        TDConnection conn =
                new TDConnection("jdbc:td://192.168.0.23:80/mugadb", props);
        String sql = "insert into table02 (k1, k2, k3) values (?, 1, ?)";
        TDPreparedStatement ps = (TDPreparedStatement) conn.prepareStatement(sql);
        for (int i = 0; i < 10; i++) {
            ps.setString(1, "muga:" + i);
            ps.setInt(2, i);
            ps.execute();
        }
        ps.getCommandExecutor().getAPI().flush();
        System.out.println("fin");
    }

    @Test
    public void returnNullForNonSupportedJDBC()
            throws SQLException
    {
        Connection conn = TreasureDataDriver.getConnection("jdbc:mysql://localhost:1234/dbname", new Properties());
        Assert.assertNull(conn);
    }

    @Test
    public void supportMultipleJDBCDriverInClasspaths()
            throws SQLException
    {
        try {
            Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:99999/dbname", new Properties());
        }
        catch (CommunicationsException e) {
            // OK since mysql doesn't exist
        }
    }
}
