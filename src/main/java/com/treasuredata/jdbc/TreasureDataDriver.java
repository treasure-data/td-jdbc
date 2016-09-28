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
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * TreasureDataDriver.
 *
 */
public class TreasureDataDriver
        implements Driver
{
    private static Logger LOG = Logger.getLogger(
            TreasureDataDriver.class.getName());

    public static TreasureDataDriver driverInstance;

    static {
        try {
            driverInstance = new TreasureDataDriver();
            DriverManager.registerDriver(driverInstance);
        }
        catch (Exception e) {
            LOG.severe(String.format("%s: %s",
                    e.getClass().getName(), e.getMessage()));
        }
    }

    public TreasureDataDriver()
    {
    }

    public Connection connect(String url, Properties props)
            throws SQLException
    {
        return getConnection(url, props);
    }

    public static Connection getConnection(String url, Properties props)
            throws SQLException
    {
        if(!Config.isValidJDBCUrl(url)) {
            // Non supported driver
            return null;
        }

        if (props == null) {
            throw new SQLException("invalid arguments: properties is null");
        }

        // a connection object is not singleton
        Config config = Config.parseJdbcURL(url).setProperties(props);
        return new TDConnection(config);
    }

    public boolean acceptsURL(String url)
            throws SQLException
    {
        if (url == null) {
            return false;
        }

        if (url.regionMatches(true, 0, Constants.URL_PREFIX0, 0,
                Constants.URL_PREFIX0.length())) {
            return true;
        }

        return false;
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties props)
            throws SQLException
    {
        if (!acceptsURL(url)) {
            return new DriverPropertyInfo[0];
        }

        DriverPropertyInfo[] props0 = new DriverPropertyInfo[2];
        DriverPropertyInfo p;

        if (props == null) {
            props = new Properties();
        }

        p = new DriverPropertyInfo("user", null);
        p.value = props.getProperty("user");
        p.required = true;
        p.description = "user's email address for accessing TreasureData Cloud";
        props0[0] = p;

        p = new DriverPropertyInfo("host", null);
        p.value = props.getProperty("host");
        p.required = true;
        p.description = "host name of TreasureData Cloud";
        props0[1] = p;

        return props0;
    }

    public int getMajorVersion()
    {
        return Constants.DRIVER_MAJOR_VERSION;
    }

    public int getMinorVersion()
    {
        return Constants.DRIVER_MINOR_VERSION;
    }

    public boolean jdbcCompliant()
    {
        return Constants.JDBC_COMPLIANT;
    }
}
