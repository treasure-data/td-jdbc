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

import com.treasuredata.jdbc.command.ClientAPI;
import com.treasuredata.jdbc.command.TDClientAPI;
import com.treasure_data.model.Database;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;

public class TDConnection
        implements Connection, Constants
{
    private final Config config;

    private boolean autoCommit = false;

    private boolean readOnly = false;

    private ClientAPI api;

    private Database database;

    private int maxRows = 5000;

    private SQLWarning warnings = null;

    public TDConnection()
            throws SQLException {
        this(new ConfigBuilder().createConnectionConfig());
    }

    public TDConnection(String url, Properties props)
         throws SQLException {
        this(Config.newConfig(url, props));
    }

    public TDConnection(Config config)
            throws SQLException  {
        this.config = config;
        // create a Database object
        database = new Database(config.database);

        // create a ClientAPI object
        api = new TDClientAPI(this);
    }

    public Config getConfig()
    {
        return config;
    }

    public ClientAPI getClientAPI()
    {
        return api;
    }

    public Database getDatabase()
    {
        return database;
    }

    public int getMaxRows()
    {
        return maxRows;
    }

    public void clearWarnings()
            throws SQLException
    {
        warnings = null;
    }

    public SQLWarning getWarnings()
            throws SQLException
    {
        return warnings;
    }

    public void setWarning(SQLWarning w)
            throws SQLException
    {
        if (w == null) {
            throw new SQLException("a SQLWarning object is null");
        }

        if (warnings == null) {
            warnings = w;
        }
        else {
            warnings.setNextWarning(w);
        }
    }

    public void close()
            throws SQLException
    {
        // basically ignore
        try {
            api.close();
        }
        catch (Exception e) {
            throw new SQLException(e);
        }
    }

    public boolean isClosed()
            throws SQLException
    {
        return false;
    }

    public void commit()
            throws SQLException
    {
        // ignore
    }

    public Array createArrayOf(String typeName, Object[] elements)
            throws SQLException
    {
        throw new SQLException(new UnsupportedOperationException(
                "TDConnection#createArrayOf(String, Object[])"));
    }

    public Blob createBlob()
            throws SQLException
    {
        throw new SQLException(new UnsupportedOperationException(
                "TDConnection#createBlob()"));
    }

    public Clob createClob()
            throws SQLException
    {
        throw new SQLException(new UnsupportedOperationException(
                "TDConnection#createClob()"));
    }

    public NClob createNClob()
            throws SQLException
    {
        throw new SQLException(new UnsupportedOperationException(
                "TDConnection#createNClob()"));
    }

    public SQLXML createSQLXML()
            throws SQLException
    {
        throw new SQLException(new UnsupportedOperationException(
                "TDConnection#createSQLXML()"));
    }

    /**
     * Creates a Statement object for sending SQL statements to the database.
     *
     * @throws SQLException if a database access error occurs.
     * @see java.sql.Connection#createStatement()
     */
    public Statement createStatement()
            throws SQLException
    {
        // *skip* checking if database is null or not. It is because
        // it was processed when creating a connection.
        return new TDStatement(this);
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws SQLException
    {
        return createStatement();
    }

    public Statement createStatement(int resultSetType,
            int resultSetConcurrency, int resultSetHoldability)
            throws SQLException
    {
        return createStatement();
    }

    public Struct createStruct(String typeName, Object[] attributes)
            throws SQLException
    {
        throw new SQLException(new UnsupportedOperationException(
                "TDConnection#createStruct(String, Object[])"));
    }

    public boolean getAutoCommit()
            throws SQLException
    {
        return autoCommit;
    }

    public String getCatalog()
            throws SQLException
    {
        return "";
    }

    public Properties getClientInfo()
            throws SQLException
    {
        throw new SQLException(new UnsupportedOperationException(
                "TDConnection#getClientInfo()"));
    }

    public String getClientInfo(String name)
            throws SQLException
    {
        throw new SQLException(new UnsupportedOperationException(
                "TDConnection#getClientInfo(String)"));
    }

    public int getHoldability()
            throws SQLException
    {
        throw new SQLException(new UnsupportedOperationException(
                "TDConnection#getHoldability()"));
    }

    public DatabaseMetaData getMetaData()
            throws SQLException
    {
        return new TDDatabaseMetaData(database, getClientAPI());
    }

    public int getTransactionIsolation()
            throws SQLException
    {
        return Connection.TRANSACTION_NONE;
    }

    public Map<String, Class<?>> getTypeMap()
            throws SQLException
    {
        throw new SQLException(new UnsupportedOperationException(
                "TDConnection#getTypeMap()"));
    }

    public boolean isReadOnly()
            throws SQLException
    {
        return readOnly;
    }

    public boolean isValid(int timeout)
            throws SQLException
    {
        throw new SQLException(new UnsupportedOperationException(
                "TDConnection#isValid(int)"));
    }

    public String nativeSQL(String sql)
            throws SQLException
    {
        throw new SQLException(new UnsupportedOperationException(
                "TDConnection#nativeSQL(String)"));
    }

    public CallableStatement prepareCall(String sql)
            throws SQLException
    {
        throw new SQLException(new UnsupportedOperationException(
                "TDConnection#prepareCall(String)"));
    }

    public CallableStatement prepareCall(String sql, int resultSetType,
            int resultSetConcurrency)
            throws SQLException
    {
        throw new SQLException(new UnsupportedOperationException(
                "TDConnection#prepareCall(String, int, int)"));
    }

    public CallableStatement prepareCall(String sql, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability)
            throws SQLException
    {
        throw new SQLException(new UnsupportedOperationException(
                "TDConnection#prepareCall(String, int, int, int)"));
    }

    public PreparedStatement prepareStatement(String sql)
            throws SQLException
    {
        return new TDPreparedStatement(this, sql);
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
            throws SQLException
    {
        return prepareStatement(sql);
    }

    public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
            throws SQLException
    {
        return prepareStatement(sql);
    }

    public PreparedStatement prepareStatement(String sql, String[] columnNames)
            throws SQLException
    {
        return prepareStatement(sql);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType,
            int resultSetConcurrency)
            throws SQLException
    {
        // TODO required by WingArc
        return prepareStatement(sql);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability)
            throws SQLException
    {
        return prepareStatement(sql);
    }

    public void releaseSavepoint(Savepoint savepoint)
            throws SQLException
    {
        throw new SQLException(new UnsupportedOperationException(
                "TDConnection#releaseSavepoint(Savepoint)"));
    }

    public void rollback()
            throws SQLException
    {
        // ignore
    }

    public void rollback(Savepoint savepoint)
            throws SQLException
    {
        // ignore
    }

    public void setAutoCommit(boolean autoCommit)
            throws SQLException
    {
        this.autoCommit = autoCommit;
    }

    public void setCatalog(String catalog)
            throws SQLException
    {
        throw new SQLException(new UnsupportedOperationException(
                "TDConnection#setCatalog(String)"));
    }

    public void setClientInfo(Properties properties)
            throws SQLClientInfoException
    {
        throw new SQLClientInfoException(
                "Method not supported: TDConnection#setClientInfo(Properties)",
                null);
    }

    public void setClientInfo(String name, String value)
            throws SQLClientInfoException
    {
        throw new SQLClientInfoException(
                "Method not supported: TDConnection#setClientInfo(String, String)",
                null);
    }

    public void setHoldability(int holdability)
            throws SQLException
    {
        throw new SQLException(new UnsupportedOperationException(
                "TDConnection#setHoldability()"));
    }

    public void setReadOnly(boolean readOnly)
            throws SQLException
    {
        this.readOnly = readOnly;
    }

    public Savepoint setSavepoint()
            throws SQLException
    {
        throw new SQLException(new UnsupportedOperationException(
                "TDConnection#setSavepoint()"));
    }

    public Savepoint setSavepoint(String name)
            throws SQLException
    {
        throw new SQLException(new UnsupportedOperationException(
                "TDConnection#setSavepoint(String)"));
    }

    public void setTransactionIsolation(int level)
            throws SQLException
    {
        throw new SQLException(new UnsupportedOperationException(
                "TDConnection#setTransactionIsolation(int)"));
    }

    public void setTypeMap(Map<String, Class<?>> map)
            throws SQLException
    {
        throw new SQLException(new UnsupportedOperationException(
                "TDConnection#setTypeMap(Map)"));
    }

    public boolean isWrapperFor(Class<?> iface)
            throws SQLException
    {
        throw new SQLException(new UnsupportedOperationException(
                "TDConnection#isWrapperFor(Class)"));
    }

    public <T> T unwrap(Class<T> iface)
            throws SQLException
    {
        throw new SQLException(new UnsupportedOperationException(
                "TDConnection#unwrap(Class)"));
    }
}
