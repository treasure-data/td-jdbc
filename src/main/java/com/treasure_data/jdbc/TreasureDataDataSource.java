package com.treasure_data.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

public class TreasureDataDataSource implements DataSource {

    public TreasureDataDataSource() {
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.sql.DataSource#getConnection()
     */
    public Connection getConnection() throws SQLException {
        return getConnection("", "");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.sql.DataSource#getConnection(java.lang.String,
     * java.lang.String)
     */
    public Connection getConnection(String username, String password)
            throws SQLException {
        try {
            return new TreasureDataConnection("", null);
        } catch (Exception ex) {
            throw new SQLException("Error in getting TreasureDataConnection",
                    ex);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.sql.CommonDataSource#getLogWriter()
     */
    public PrintWriter getLogWriter() throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.sql.CommonDataSource#getLoginTimeout()
     */
    public int getLoginTimeout() throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.sql.CommonDataSource#setLogWriter(java.io.PrintWriter)
     */
    public void setLogWriter(PrintWriter arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.sql.CommonDataSource#setLoginTimeout(int)
     */
    public void setLoginTimeout(int arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
     */
    public boolean isWrapperFor(Class<?> arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Wrapper#unwrap(java.lang.Class)
     */
    public <T> T unwrap(Class<T> arg0) throws SQLException {
        throw new SQLException("Method not supported");
    }

}
