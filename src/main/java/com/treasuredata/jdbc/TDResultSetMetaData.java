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

import com.treasuredata.jdbc.model.TDColumn;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

public class TDResultSetMetaData
        implements java.sql.ResultSetMetaData
{
    private final String jobId;
    private final List<String> columnNames;
    private final List<String> columnTypes;

    public TDResultSetMetaData(List<String> columnNames,
            List<String> columnTypes)
    {
        this("", columnNames, columnTypes);
    }

    public TDResultSetMetaData(String jobId, List<String> columnNames,
            List<String> columnTypes)
    {
        this.jobId = jobId;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
    }

    public String getJobId() {
        return jobId;
    }

    public String getCatalogName(int column)
            throws SQLException
    {
        throw new SQLException("Method not supported");
    }

    public String getColumnClassName(int column)
            throws SQLException
    {
        throw new SQLException("Method not supported");
    }

    public int getColumnCount()
            throws SQLException
    {
        return columnNames.size();
    }

    public int getColumnDisplaySize(int column)
            throws SQLException
    {
        int columnType = getColumnType(column);

        return TDColumn.columnDisplaySize(columnType);
    }

    public String getColumnLabel(int column)
            throws SQLException
    {
        return columnNames.get(column - 1);
    }

    public String getColumnName(int column)
            throws SQLException
    {
        return columnNames.get(column - 1);
    }

    public int getColumnType(int column)
            throws SQLException
    {
        if (columnTypes == null) {
            throw new SQLException(
                    "Could not determine column type name for ResultSet");
        }

        if (column < 1 || column > columnTypes.size()) {
            throw new SQLException("Invalid column value: " + column);
        }

        // we need to convert the thrift type to the SQL type
        String type = columnTypes.get(column - 1);

        // we need to convert the thrift type to the SQL type
        return Utils.TDTypeToSqlType(type);
    }

    public String getColumnTypeName(int column)
            throws SQLException
    {
        if (columnTypes == null) {
            throw new SQLException(
                    "Could not determine column type name for ResultSet");
        }

        if (column < 1 || column > columnTypes.size()) {
            throw new SQLException("Invalid column value: " + column);
        }

        // we need to convert the Hive type to the SQL type name
        // TODO: this would be better handled in an enum
        String type = columnTypes.get(column - 1);
        if ("string".equalsIgnoreCase(type)) {
            return Constants.STRING_TYPE_NAME;
        }
        else if ("varchar".equalsIgnoreCase(type)) {
            return Constants.VARCHAR_TYPE_NAME;
        }
        else if ("float".equalsIgnoreCase(type)) {
            return Constants.FLOAT_TYPE_NAME;
        }
        else if ("double".equalsIgnoreCase(type)) {
            return Constants.DOUBLE_TYPE_NAME;
        }
        else if ("boolean".equalsIgnoreCase(type)) {
            return Constants.BOOLEAN_TYPE_NAME;
        }
        else if ("tinyint".equalsIgnoreCase(type)) {
            return Constants.TINYINT_TYPE_NAME;
        }
        else if ("smallint".equalsIgnoreCase(type)) {
            return Constants.SMALLINT_TYPE_NAME;
        }
        else if ("int".equalsIgnoreCase(type)) {
            return Constants.INT_TYPE_NAME;
        }
        else if ("bigint".equalsIgnoreCase(type)) {
            return Constants.BIGINT_TYPE_NAME;
        }
        else if ("date".equalsIgnoreCase(type)) {
            return Constants.DATE_TYPE_NAME;
        }
        else if ("timestamp".equalsIgnoreCase(type)) {
            return Constants.TIMESTAMP_TYPE_NAME;
        }
        else if (type.startsWith("map<")) {
            return Constants.STRING_TYPE_NAME;
        }
        else if (type.startsWith("array<")) {
            return Constants.STRING_TYPE_NAME;
        }
        else if (type.startsWith("struct<")) {
            return Constants.STRING_TYPE_NAME;
        }

        throw new SQLException("Unrecognized column type: " + type);
    }

    public int getPrecision(int column)
            throws SQLException
    {
        int columnType = getColumnType(column);

        return TDColumn.columnPrecision(columnType);
    }

    public int getScale(int column)
            throws SQLException
    {
        int columnType = getColumnType(column);

        return TDColumn.columnScale(columnType);
    }

    public String getSchemaName(int column)
            throws SQLException
    {
        throw new SQLException("Method not supported");
    }

    public String getTableName(int column)
            throws SQLException
    {
        throw new SQLException("Method not supported");
    }

    public boolean isAutoIncrement(int column)
            throws SQLException
    {
        // Hive doesn't have an auto-increment concept
        return false;
    }

    public boolean isCaseSensitive(int column)
            throws SQLException
    {
        throw new SQLException("Method not supported");
    }

    public boolean isCurrency(int column)
            throws SQLException
    {
        // Hive doesn't support a currency type
        return false;
    }

    public boolean isDefinitelyWritable(int column)
            throws SQLException
    {
        throw new SQLException("Method not supported");
    }

    public int isNullable(int column)
            throws SQLException
    {
        // Hive doesn't have the concept of not-null
        return ResultSetMetaData.columnNullable;
    }

    /*
     * Indicates whether the designated column is definitely not writable.
     * We support to read column only via JDBC Driver for now.
     *
     * @param column the first column is 1, the second is 2, etc.
     * @return true if so
     * @exception SQLException if a database access error occurs
     */
    public boolean isReadOnly(int column)
            throws SQLException
    {
        return true;
    }

    public boolean isSearchable(int column)
            throws SQLException
    {
        throw new SQLException("Method not supported");
    }

    public boolean isSigned(int column)
            throws SQLException
    {
        if (columnTypes == null) {
            throw new SQLException(
                    "Could not determine column type name for ResultSet");
        }

        if (column < 1 || column > columnTypes.size()) {
            throw new SQLException("Invalid column value: " + column);
        }

        // we need to convert the Hive type to the SQL type name
        // TODO: this would be better handled in an enum
        String type = columnTypes.get(column - 1);
        if ("string".equalsIgnoreCase(type) ||
                "varchar".equalsIgnoreCase(type) ||
                "boolean".equalsIgnoreCase(type) ||
                "date".equalsIgnoreCase(type) ||
                "timestamp".equalsIgnoreCase(type) ||
                type.startsWith("map<") ||
                type.startsWith("array<") ||
                type.startsWith("struct<")) {
            return false;
        }
        else if ("float".equalsIgnoreCase(type) ||
                "double".equalsIgnoreCase(type) ||
                "tinyint".equalsIgnoreCase(type) ||
                "smallint".equalsIgnoreCase(type) ||
                "int".equalsIgnoreCase(type) ||
                "bigint".equalsIgnoreCase(type)) {
            return true;
        }

        throw new SQLException("Unrecognized column type: " + type);
    }

    public boolean isWritable(int column)
            throws SQLException
    {
        return !isReadOnly(column);
    }

    public boolean isWrapperFor(Class<?> iface)
            throws SQLException
    {
        throw new SQLException("Method not supported");
    }

    public <T> T unwrap(Class<T> iface)
            throws SQLException
    {
        throw new SQLException("Method not supported");
    }
}
