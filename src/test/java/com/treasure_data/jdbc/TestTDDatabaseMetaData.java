package com.treasure_data.jdbc;

import com.treasure_data.client.ClientException;
import com.treasure_data.jdbc.command.NullClientAPI;
import com.treasure_data.model.Database;
import com.treasure_data.model.DatabaseSummary;
import com.treasure_data.model.Table;
import com.treasure_data.model.TableSummary;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestTDDatabaseMetaData
{

    @Test
    public void getCatalogSeparator()
            throws Exception
    {
        TDDatabaseMetaData metadata = new TDDatabaseMetaData(
                new NullClientAPI());
        assertEquals(".", metadata.getCatalogSeparator());
    }

    @Test
    public void getCatalogTerm()
            throws Exception
    {
        TDDatabaseMetaData metadata = new TDDatabaseMetaData(
                new NullClientAPI());
        assertEquals("database", metadata.getCatalogTerm());
    }

    @Test
    public void getCatalogs()
            throws Exception
    {
        NullClientAPI api = new NullClientAPI()
        {
            @Override
            public DatabaseSummary showDatabase()
                    throws ClientException
            {
                return new DatabaseSummary("db01", 10, "created_at01",
                        "updated_at01");
            }
        };

        {
            TDDatabaseMetaData metadata = new TDDatabaseMetaData(api);
            ResultSet rs = null;
            try {
                rs = metadata.getCatalogs();

                try {
                    rs.getString("TABLE_CAT");
                    fail();
                }
                catch (Throwable t) {
                    assertTrue(t instanceof SQLException);
                }

                assertTrue(rs.next());
                assertEquals("db01", rs.getString(1));
                assertEquals("db01", rs.getString("TABLE_CAT"));

                try {
                    rs.getString("notfound");
                }
                catch (Throwable t) {
                    assertTrue(t instanceof SQLException);
                }

                assertFalse(rs.next());
            }
            finally {
                if (rs != null) {
                    rs.close();
                }
            }
        }
    }

    @Test
    public void getColumns()
            throws Exception
    {
        NullClientAPI api = new NullClientAPI()
        {
            @Override
            public List<TableSummary> showTables()
                    throws ClientException
            {
                List<TableSummary> list = new ArrayList<TableSummary>();
                list.add(new TableSummary(
                        new Database("mugadb"),
                        "tbl01",
                        12344,
                        "[[\"f01\",\"string\"],[\"f02\",\"int\"],[\"f03\",\"long\"]]",
                        "2012-02-20T18:31:48Z", "2012-02-20T18:31:48Z"));
                list.add(new TableSummary(new Database("mugadb"), "tbl02",
                        12344, "[]", "2012-02-20T18:31:48Z",
                        "2012-02-20T18:31:48Z"));
                list.add(new TableSummary(new Database("mugadb"), "tbl03", Table.Type.ITEM,
                        12344, "[]", "2012-02-20T18:31:48Z",
                        "2012-02-20T18:31:48Z"));
                return list;
            }
        };

        {
            TDDatabaseMetaData metadata = new TDDatabaseMetaData(api);
            ResultSet rs = null;
            try {
                rs = metadata.getColumns(null, null, null, null);

                try {
                    rs.getString(1);
                    fail();
                }
                catch (Throwable t) {
                    assertTrue(t instanceof SQLException);
                }

                assertTrue(rs.next());
                assertEquals("default", rs.getString("TABLE_CAT"));
                assertEquals(null, rs.getString("TABLE_SCHEM"));
                assertEquals("tbl01", rs.getString("TABLE_NAME"));
                assertEquals("f01", rs.getString("COLUMN_NAME"));
                assertEquals("string", rs.getString("TYPE_NAME"));

                assertTrue(rs.next());
                assertEquals("default", rs.getString("TABLE_CAT"));
                assertEquals(null, rs.getString("TABLE_SCHEM"));
                assertEquals("tbl01", rs.getString("TABLE_NAME"));
                assertEquals("f02", rs.getString("COLUMN_NAME"));
                assertEquals("int", rs.getString("TYPE_NAME"));

                assertTrue(rs.next());
                assertEquals("default", rs.getString("TABLE_CAT"));
                assertEquals(null, rs.getString("TABLE_SCHEM"));
                assertEquals("tbl01", rs.getString("TABLE_NAME"));
                assertEquals("f03", rs.getString("COLUMN_NAME"));
                assertEquals("long", rs.getString("TYPE_NAME"));

                assertTrue(rs.next());
                assertEquals("default", rs.getString("TABLE_CAT"));
                assertEquals(null, rs.getString("TABLE_SCHEM"));
                assertEquals("tbl01", rs.getString("TABLE_NAME"));
                assertEquals("time", rs.getString("COLUMN_NAME"));
                assertEquals("int", rs.getString("TYPE_NAME"));

                assertTrue(rs.next());
                assertEquals("default", rs.getString("TABLE_CAT"));
                assertEquals(null, rs.getString("TABLE_SCHEM"));
                assertEquals("tbl02", rs.getString("TABLE_NAME"));
                assertEquals("time", rs.getString("COLUMN_NAME"));
                assertEquals("int", rs.getString("TYPE_NAME"));

                assertFalse(rs.next());
            }
            finally {
                if (rs != null) {
                    rs.close();
                }
            }
        }
    }

    @Test
    public void getTables()
            throws Exception
    {
        NullClientAPI api = new NullClientAPI()
        {
            @Override
            public List<TableSummary> showTables()
                    throws ClientException
            {
                List<TableSummary> list = new ArrayList<TableSummary>();
                list.add(new TableSummary(
                        new Database("mugadb"),
                        "tbl01",
                        12344,
                        "[[\"f01\",\"string\"],[\"f02\",\"int\"],[\"f03\",\"long\"]]",
                        "2012-02-20T18:31:48Z", "2012-02-20T18:31:48Z"));
                list.add(new TableSummary(new Database("mugadb"), "tbl02",
                        12344, "[]", "2012-02-20T18:31:48Z",
                        "2012-02-20T18:31:48Z"));
                return list;
            }
        };

        {
            TDDatabaseMetaData metadata = new TDDatabaseMetaData(new Database("mugadb"), api);
            ResultSet rs = null;
            try {
                rs = metadata.getTables(null, null, null, null);
                assertTrue(rs.next());
                assertEquals("mugadb", rs.getString("TABLE_CAT"));
                assertEquals(null, rs.getString("TABLE_SCHEM"));
                assertEquals("tbl01", rs.getString("TABLE_NAME"));
                assertEquals("TABLE", rs.getString("TABLE_TYPE"));

                assertTrue(rs.next());
                assertEquals("mugadb", rs.getString("TABLE_CAT"));
                assertEquals(null, rs.getString("TABLE_SCHEM"));
                assertEquals("tbl02", rs.getString("TABLE_NAME"));
                assertEquals("TABLE", rs.getString("TABLE_TYPE"));

                assertFalse(rs.next());
            }
            finally {
                if (rs != null) {
                    rs.close();
                }
            }
        }
    }
}
