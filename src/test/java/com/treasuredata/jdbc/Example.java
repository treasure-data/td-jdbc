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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * td-jdbc usage example
 */
public class Example
{
    public static void basicUsage()
            throws SQLException
    {
        Connection conn = DriverManager.getConnection(
                "jdbc:td://api.treasuredata.com/sample_datasets",
                "(your account email address)",
                "(your account password)");
        Statement st = conn.createStatement();
        try {
            ResultSet rs = st.executeQuery("SELECT count(1) FROM www_access");
            while (rs.next()) {
                int count = rs.getInt(1);
                System.out.println("result = " + count);
            }
            rs.close();
        }
        finally {
            st.close();
            conn.close();
        }
    }

    public static void propertyConfig()
            throws SQLException
    {
        Properties props = new Properties();
        props.setProperty("user", "(your account e-mail)");
        props.setProperty("password", "(your password)");

        // Set the other options

        // Use SSL (default) or not
        // props.setProperty("useSSL", "true");

        // Run Hive jobs. The default is "presto"
        // props.setProperty("type", "hive");

        // proxy configurarion (optional)
        // props.setProperty("httpproxyhost", "(proxy host)");
        // props.setProperty("httpproxyport", "(proxy port)");
        // props.setProperty("httpproxyuser", "(proxy username)");
        // props.setProperty("httpproxypassword", "(proxy password)");

        Connection conn = DriverManager.getConnection(
                "jdbc:td://api.treasuredata.com/sample_datasets",
                props
        );
        Statement st = conn.createStatement();
        try {
            ResultSet rs = st.executeQuery("SELECT count(1) FROM www_access");
            while (rs.next()) {
                int count = rs.getInt(1);
                System.out.println("result = " + count);
            }
            rs.close();
        }
        finally {
            st.close();
            conn.close();
        }
    }

    public static void connectWithApiKey()
            throws SQLException
    {
        Properties props = new Properties();
        props.setProperty("apikey", "(your API key)");

        Connection conn = DriverManager.getConnection(
                "jdbc:td://api.treasuredata.com/sample_datasets",
                props
        );
        Statement st = conn.createStatement();
        try {
            ResultSet rs = st.executeQuery("SELECT count(1) FROM www_access");
            while (rs.next()) {
                int count = rs.getInt(1);
                System.out.println("result = " + count);
            }
            rs.close();
        }
        finally {
            st.close();
            conn.close();
        }
    }
}