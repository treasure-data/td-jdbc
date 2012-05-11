package com.treasure_data.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;

import org.hsqldb.result.ResultConstants;

import com.treasure_data.jdbc.command.Wrapper;

public class TDPreparedStatement extends TDStatement implements PreparedStatement {

    private Wrapper w;

    private final HashMap<Integer, Object> params = new HashMap<Integer, Object>();

    public TDPreparedStatement(TDConnection conn, String sql)
            throws SQLException {
        super(conn);
        w = fetchResult(sql, ResultConstants.PREPARE);
    }

    public void clearParameters() throws SQLException {
        params.clear();
    }

    public void addBatch() throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public boolean execute() throws SQLException {
        return executeQuery() != null;
    }

    public synchronized ResultSet executeQuery() throws SQLException {
        w.params = params;
        fetchResult(w);
        return getResultSet();
    }

    public int executeUpdate() throws SQLException {
        executeQuery();
        return getUpdateCount();
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public void setArray(int i, Array x) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public void setAsciiStream(int i, InputStream in) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public void setAsciiStream(int i, InputStream in, int length) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public void setAsciiStream(int i, InputStream in, long length) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public void setBigDecimal(int parameterIndex, BigDecimal x)
            throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public void setBinaryStream(int i, InputStream x) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public void setBinaryStream(int i, InputStream in, int length) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public void setBinaryStream(int i, InputStream in, long length) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public void setBlob(int i, Blob x) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public void setBlob(int i, InputStream inputStream) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public void setBlob(int i, InputStream inputStream, long length) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        this.params.put(parameterIndex, "" + x);
    }

    public void setByte(int i, byte x) throws SQLException {
        this.params.put(i, "" + x);
    }

    public void setBytes(int i, byte[] x) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public void setCharacterStream(int i, Reader reader) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public void setCharacterStream(int i, Reader reader, int length) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public void setCharacterStream(int i, Reader reader, long length) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public void setClob(int i, Clob x) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public void setClob(int i, Reader reader) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public void setClob(int i, Reader reader, long length) throws SQLException {
        throw new SQLException(new UnsupportedOperationException());
    }

    public void setDate(int i, Date x) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setDate(int i, Date x, Calendar cal) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setDouble(int i, double x) throws SQLException {
        params.put(i, "" + x);
    }

    public void setFloat(int i, float x) throws SQLException {
        params.put(i, "" + x);
    }

    public void setInt(int i, int x) throws SQLException {
        params.put(i, "" + x);
    }

    public void setLong(int i, long x) throws SQLException {
        params.put(i, "" + x);
    }

    public void setNCharacterStream(int i, Reader value) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setNCharacterStream(int i, Reader value, long length)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setNClob(int i, NClob value) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setNClob(int i, Reader reader) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setNClob(int i, Reader reader, long length)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setNString(int i, String value)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setNull(int i, int sqlType) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setNull(int i, int sqlType, String typeName) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setObject(int i, Object x) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setObject(int i, Object x, int targetSqlType) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setObject(int i, Object x, int targetSqlType, int scale)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setRef(int i, Ref x) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setRowId(int i, RowId x) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setSQLXML(int i, SQLXML xmlObject)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setShort(int i, short x) throws SQLException {
        params.put(i, "" + x);
    }

    public void setString(int i, String x) throws SQLException {
        x = x.replace("'", "\\'");
        params.put(i, "'" + x + "'");
    }

    public void setTime(int i, Time x) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setTime(int i, Time x, Calendar cal)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setTimestamp(int i, Timestamp x)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setTimestamp(int i, Timestamp x, Calendar cal)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setURL(int i, URL x) throws SQLException {
        throw new SQLException("Method not supported");
    }

    public void setUnicodeStream(int i, InputStream x, int length)
            throws SQLException {
        throw new SQLException("Method not supported");
    }

}
