package com.treasure_data.jdbc;

import org.junit.Test;
import java.util.HashMap;
import java.util.Map;

import static junit.framework.Assert.assertEquals;

public class TestTDPreparedStatement
{

    private TDPreparedStatement createPreparedStatement(String query)
            throws Exception
    {
        TDConnection conn = (TDConnection) TestProductionEnv.newPrestoConnection("default");
        return new TDPreparedStatement(conn, query);
    }

    private void assertReplacedQuery(String prepared, Map<Integer, String> params,
            String expected)
            throws Exception
    {
        TDPreparedStatement stat = createPreparedStatement(prepared);
        assertEquals(stat.updateSql(prepared, params), expected);
    }

    @Test
    public void updateSql()
            throws Exception
    {
        // Oh boy, no such placeholder location...
        String actual = "SELECT ?, ? FROM table";
        Map<Integer, String> params = new HashMap<Integer, String>();
        params.put(1, "'foo'");
        params.put(2, "'bar'");
        assertReplacedQuery(actual, params, "SELECT 'foo', 'bar' FROM table");
    }

    @Test
    public void updateSqlWithQuotedChar()
            throws Exception
    {
        String actual = "select * from www_access where path like ? or host = ?";
        Map<Integer, String> params = new HashMap<Integer, String>();
        params.put(1, "'\\%foo'");
        params.put(2, "'\\%bar'");
        assertReplacedQuery(actual, params, "select * from www_access where path like '\\%foo' or host = '\\%bar'");
    }
}
