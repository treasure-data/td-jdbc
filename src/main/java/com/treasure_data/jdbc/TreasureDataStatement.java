package com.treasure_data.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import org.apache.hadoop.hive.jdbc.HiveQueryResultSet;

import com.treasure_data.client.ClientException;
import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.model.Database;
import com.treasure_data.model.Job;
import com.treasure_data.model.SubmitJobRequest;
import com.treasure_data.model.SubmitJobResult;

public class TreasureDataStatement implements Statement {

    private TreasureDataClient client;

    private int fetchSize = 50;

    /**
     * The maximum number of rows this statement should return (0 => all rows).
     */
    private int maxRows = 0;

    /**
     * We need to keep a reference to the result set to support the following:
     * <code>
     * statement.execute(String sql);
     * statement.getResultSet();
     * </code>.
     */
    private ResultSet resultSet = null;

    /**
     * Add SQLWarnings to the warningChain if needed.
     */
    private SQLWarning warningChain = null;

    /**
     * Keep state so we can fail certain calls made after close().
     */
    private boolean isClosed = false;

    public TreasureDataStatement(TreasureDataClient client) {
        this.client = client;
    }

    @Override
    public boolean isWrapperFor(Class<?> arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void addBatch(String arg0) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void cancel() throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void clearBatch() throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void clearWarnings() throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void close() throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean execute(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean execute(String arg0, int arg1) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean execute(String arg0, int[] arg1) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean execute(String arg0, String[] arg1) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int[] executeBatch() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#executeQuery(java.lang.String)
     */

    public ResultSet executeQuery(String sql) throws SQLException {
        // TODO #MN
        if (isClosed) {
            throw new SQLException("Can't execute after statement has been closed");
        }

        Job job = null;
        try {
            resultSet = null;
            {
                Database database = new Database("mugadb");
                //String q = "select * from mugatbl";
                String q = sql;
                SubmitJobRequest request = new SubmitJobRequest(database, q, null, false);
                SubmitJobResult result = client.submitJob(request);
                job = result.getJob();
                System.out.println(job.getJobID());
            }
            //client.execute(sql);
        } catch (ClientException e) {
            throw new SQLException(e);
            //throw new SQLException(e.getMessage(), e.getSQLState(), e.getErrorCode());
        } catch (Exception ex) {
            throw new SQLException(ex.toString(), "08S01");
        }
        //resultSet = new HiveQueryResultSet(client, maxRows);
        resultSet = new TreasureDataResultSet(client, maxRows, job);
        resultSet.setFetchSize(fetchSize);
        return resultSet;
    }

    @Override
    public int executeUpdate(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int executeUpdate(String arg0, int arg1) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int executeUpdate(String arg0, int[] arg1) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int executeUpdate(String arg0, String[] arg1) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Connection getConnection() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getFetchSize() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxRows() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean getMoreResults(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getResultSetType() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isClosed() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isPoolable() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void setCursorName(String arg0) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setEscapeProcessing(boolean arg0) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setFetchDirection(int arg0) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setFetchSize(int arg0) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setMaxFieldSize(int arg0) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setMaxRows(int arg0) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setPoolable(boolean arg0) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setQueryTimeout(int arg0) throws SQLException {
        // TODO Auto-generated method stub

    }

}
