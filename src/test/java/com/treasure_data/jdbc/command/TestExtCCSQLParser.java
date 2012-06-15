package com.treasure_data.jdbc.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import org.junit.Test;

import com.treasure_data.jdbc.command.ExtCCSQLParser;
import com.treasure_data.jdbc.compiler.parser.ParseException;
import com.treasure_data.jdbc.compiler.stat.Select;
import com.treasure_data.jdbc.compiler.stat.Show;
import com.treasure_data.jdbc.compiler.stat.Statement;

public class TestExtCCSQLParser {

    private static Statement s(String sql) throws ParseException {
        ExtCCSQLParser p = new ExtCCSQLParser(sql);
        return p.Statement();
    }

    /**
     * for pentaho report designer 3.8.3
     */
    @Test
    public void testSelectOne() throws Exception {
        {
            String sql = "select 1";
            Statement stat = s(sql);

            assertTrue(stat instanceof Select);
            Select select = (Select) stat;
            assertTrue(select.isSelectOne());
            assertEquals(sql, select.getString());
        }
        {
            String sql = "SELECT 1";
            Statement stat = s(sql);

            assertTrue(stat instanceof Select);
            Select select = (Select) stat;
            assertTrue(select.isSelectOne());
            assertEquals(sql, select.getString());
        }
        {
            String sql = "Select 1";
            Statement stat = s(sql);

            assertTrue(stat instanceof Select);
            Select select = (Select) stat;
            assertTrue(select.isSelectOne());
            assertEquals(sql, select.getString());
        }
    }

    @Test
    public void testSelect() throws Exception {
        {
            String sql = "select v from testdb";
            Statement stat = s(sql);

            assertTrue(stat instanceof Select);
            Select select = (Select) stat;
            assertFalse(select.isSelectOne());
            assertEquals(sql, select.getString());
        }
        {
            String sql = "Select v from testdb";
            Statement stat = s(sql);

            assertTrue(stat instanceof Select);
            Select select = (Select) stat;
            assertFalse(select.isSelectOne());
            assertEquals(sql, select.getString());
        }
    }

    @Test
    public void testShow() throws Exception {
        {
            String sql = "show foo";
            try {
                Statement stat = s(sql);
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof ParseException);
            }
        }
        {
            String sql = "show schemas";
            Statement stat = s(sql);
            assertTrue(stat instanceof Show);
            Show show = (Show) stat;
            assertEquals("SCHEMAS", show.getType());
            assertEquals(null, show.getParameters());
        }
        {
            String sql = "show tables";
            Statement stat = s(sql);
            assertTrue(stat instanceof Show);
            Show show = (Show) stat;
            assertEquals("TABLES", show.getType());
            assertEquals(null, show.getParameters());
        }
        {
            String sql = "show tables error";
            try {
                Statement stat = s(sql);
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof ParseException);
            }
        }
        {
            String sql = "show tables from schemaName";
            Statement stat = s(sql);
            assertTrue(stat instanceof Show);
            Show show = (Show) stat;
            assertEquals("TABLES", show.getType());
            List<String> params = show.getParameters();
            assertEquals(1, params.size());
            assertEquals("schemaName", params.get(0));
        }
        {
            String sql = "show columns from tableName";
            Statement stat = s(sql);
            assertTrue(stat instanceof Show);
            Show show = (Show) stat;
            assertEquals("COLUMNS", show.getType());
            List<String> params = show.getParameters();
            assertEquals(1, params.size());
            assertEquals("tableName", params.get(0));
        }
        {
            String sql = "show columns from tableName from schemaName";
            Statement stat = s(sql);
            assertTrue(stat instanceof Show);
            Show show = (Show) stat;
            assertEquals("COLUMNS", show.getType());
            List<String> params = show.getParameters();
            assertEquals(2, params.size());
            assertEquals("tableName", params.get(0));
            assertEquals("schemaName", params.get(1));
        }
        {
            String sql = "show columns from tableName from schemaName error";
            try {
                Statement stat = s(sql);
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof ParseException);
            }
        }
    }
}
