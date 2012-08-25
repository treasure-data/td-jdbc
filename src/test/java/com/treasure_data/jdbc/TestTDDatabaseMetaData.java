package com.treasure_data.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.treasure_data.client.ClientException;
import com.treasure_data.jdbc.command.NullClientAPI;
import com.treasure_data.model.Database;
import com.treasure_data.model.TableSummary;


public class TestTDDatabaseMetaData {

    @Test
    public void getCatalogSeparator() throws Exception {
        TDDatabaseMetaData metadata = new TDDatabaseMetaData(new NullClientAPI());
        assertEquals(".", metadata.getCatalogSeparator());
    }

    @Test
    public void getCatalogTerm() throws Exception {
        TDDatabaseMetaData metadata = new TDDatabaseMetaData(new NullClientAPI());
        assertEquals("database", metadata.getCatalogTerm());
    }

    @Test
    public void getCatalogs() throws Exception {
        TDDatabaseMetaData metadata = new TDDatabaseMetaData(new NullClientAPI());
        ResultSet rs = null;
        try {
            rs = metadata.getCatalogs();

            try {
                rs.getString("TABLE_CAT");
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }

            assertTrue(rs.next());
            assertEquals("default", rs.getString(1));
            assertEquals("default", rs.getString("TABLE_CAT"));
            assertEquals(1003, rs.getType());

            try {
                rs.getString("notfound");
            } catch (Throwable t) {
                assertTrue(t instanceof SQLException);
            }

            assertFalse(rs.next());
        } finally {
            if (rs != null) {
                rs.close();
            }
        }
    }

    @Test
    public void getColumns() throws Exception {
        class MockShowTables extends NullClientAPI {
            @Override public List<TableSummary> showTables() throws ClientException {
                List<TableSummary> list = new ArrayList<TableSummary>();
                list.add(new TableSummary(new Database("mugadb"), "tbl01", 12344,
                        "[[\"f01\",\"string\"],[\"f02\",\"int\"],[\"f03\",\"long\"]]",
                        "2012-02-20T18:31:48Z",
                        "2012-02-20T18:31:48Z"));
                list.add(new TableSummary(new Database("mugadb"), "tbl02", 12344,
                        "[]",
                        "2012-02-20T18:31:48Z",
                        "2012-02-20T18:31:48Z"));
                return list;
            }
        }

        {
            TDDatabaseMetaData metadata = new TDDatabaseMetaData(new MockShowTables());
            ResultSet rs = null;
            try {
                rs = metadata.getColumns(null, null, null, null);

                try {
                    rs.getString(1);
                } catch (Throwable t) {
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

                assertFalse(rs.next());
            } finally {
                if (rs != null) {
                    rs.close();
                }
            }
        }
    }

    @Test
    public void getTables() throws Exception {
        class MockShowTables extends NullClientAPI {
            @Override public List<TableSummary> showTables() throws ClientException {
                List<TableSummary> list = new ArrayList<TableSummary>();
                list.add(new TableSummary(new Database("mugadb"), "tbl01", 12344,
                        "[[\"f01\",\"string\"],[\"f02\",\"int\"],[\"f03\",\"long\"]]",
                        "2012-02-20T18:31:48Z",
                        "2012-02-20T18:31:48Z"));
                list.add(new TableSummary(new Database("mugadb"), "tbl02", 12344,
                        "[]",
                        "2012-02-20T18:31:48Z",
                        "2012-02-20T18:31:48Z"));
                return list;
            }
        }

        {
            TDDatabaseMetaData metadata = new TDDatabaseMetaData(new MockShowTables());
            ResultSet rs = null;
            try {
                rs = metadata.getTables(null, null, null, null);

                try {
                    rs.getString(1);
                } catch (Throwable t) {
                    assertTrue(t instanceof SQLException);
                }

                assertTrue(rs.next());
                assertEquals("default", rs.getString("TABLE_CAT"));
                assertEquals(null, rs.getString("TABLE_SCHEM"));
                assertEquals("tbl01", rs.getString("TABLE_NAME"));
                assertEquals("TABLE", rs.getString("TABLE_TYPE"));

                assertTrue(rs.next());
                assertEquals("default", rs.getString("TABLE_CAT"));
                assertEquals(null, rs.getString("TABLE_SCHEM"));
                assertEquals("tbl02", rs.getString("TABLE_NAME"));
                assertEquals("TABLE", rs.getString("TABLE_TYPE"));

                assertFalse(rs.next());
            } finally {
                if (rs != null) {
                    rs.close();
                }
            }
        }
    }
}
