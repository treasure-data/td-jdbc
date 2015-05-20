package com.treasure_data.jdbc.model;

import com.treasure_data.jdbc.Utils;

import java.sql.SQLException;
import java.sql.Types;

public class TDColumn
{
    private final String columnName;

    private final String tableName;

    private final String tableCatalog;

    private final String type;

    private final String comment;

    private final int ordinal;

    public TDColumn(String columnName, String tableName, String tableCatalog,
            String type, String comment, int ordinal)
    {
        this.columnName = columnName;
        this.tableName = tableName;
        this.tableCatalog = tableCatalog;
        this.type = type;
        this.comment = comment;
        this.ordinal = ordinal;
    }

    public String getColumnName()
    {
        return columnName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public String getTableCatalog()
    {
        return tableCatalog;
    }

    public String getType()
    {
        return type;
    }

    public Integer getSqlType()
            throws SQLException
    {
        return Utils.TDTypeToSqlType(type);
    }

    public static int columnDisplaySize(int columnType)
            throws SQLException
    {
        // according to hiveTypeToSqlType possible options are:
        switch (columnType) {
            case Types.BOOLEAN:
                return columnPrecision(columnType);
            case Types.VARCHAR:
                return Integer.MAX_VALUE; // hive has no max limit for strings
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
                return columnPrecision(columnType) + 1; // allow +/-

            // see
            // http://download.oracle.com/javase/6/docs/api/constant-values.html#java.lang.Float.MAX_EXPONENT
            case Types.FLOAT:
                return 24; // e.g. -(17#).e-###
            // see
            // http://download.oracle.com/javase/6/docs/api/constant-values.html#java.lang.Double.MAX_EXPONENT
            case Types.DOUBLE:
                return 25; // e.g. -(17#).e-####
            default:
                throw new SQLException("Invalid column type: " + columnType);
        }
    }

    public static int columnPrecision(int columnType)
            throws SQLException
    {
        // according to hiveTypeToSqlType possible options are:
        switch (columnType) {
            case Types.BOOLEAN:
                return 1;
            case Types.VARCHAR:
                return Integer.MAX_VALUE; // hive has no max limit for strings
            case Types.TINYINT:
                return 3;
            case Types.SMALLINT:
                return 5;
            case Types.INTEGER:
                return 10;
            case Types.BIGINT:
                return 19;
            case Types.FLOAT:
                return 7;
            case Types.DOUBLE:
                return 15;
            default:
                throw new SQLException("Invalid column type: " + columnType);
        }
    }

    public static int columnScale(int columnType)
            throws SQLException
    {
        // according to hiveTypeToSqlType possible options are:
        switch (columnType) {
            case Types.BOOLEAN:
            case Types.VARCHAR:
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
                return 0;
            case Types.FLOAT:
                return 7;
            case Types.DOUBLE:
                return 15;
            default:
                throw new SQLException("Invalid column type: " + columnType);
        }
    }

    public Integer getColumnSize()
            throws SQLException
    {
        int precision = columnPrecision(Utils.TDTypeToSqlType(type));
        return precision == 0 ? null : precision;
    }

    public Integer getDecimalDigits()
            throws SQLException
    {
        return columnScale(Utils.TDTypeToSqlType(type));
    }

    public Integer getNumPrecRadix()
    {
        if (type.equalsIgnoreCase("tinyint")) {
            return 10;
        }
        else if (type.equalsIgnoreCase("smallint")) {
            return 10;
        }
        else if (type.equalsIgnoreCase("int")) {
            return 10;
        }
        else if (type.equalsIgnoreCase("bigint")) {
            return 10;
        }
        else if (type.equalsIgnoreCase("float")) {
            return 2;
        }
        else if (type.equalsIgnoreCase("double")) {
            return 2;
        }
        else { // anything else including boolean and string is null
            return null;
        }
    }

    public String getComment()
    {
        return comment;
    }

    public int getOrdinal()
    {
        return ordinal;
    }
}
