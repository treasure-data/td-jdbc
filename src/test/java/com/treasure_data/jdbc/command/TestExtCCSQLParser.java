package com.treasure_data.jdbc.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import org.junit.Test;

import com.treasure_data.jdbc.command.ExtCCSQLParser;
import com.treasure_data.jdbc.compiler.expr.Expression;
import com.treasure_data.jdbc.compiler.expr.LongValue;
import com.treasure_data.jdbc.compiler.expr.StringValue;
import com.treasure_data.jdbc.compiler.expr.ops.ExpressionList;
import com.treasure_data.jdbc.compiler.parser.ParseException;
import com.treasure_data.jdbc.compiler.schema.Column;
import com.treasure_data.jdbc.compiler.stat.ColumnDefinition;
import com.treasure_data.jdbc.compiler.stat.CreateTable;
import com.treasure_data.jdbc.compiler.stat.Drop;
import com.treasure_data.jdbc.compiler.stat.Insert;
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
    public void testSelectPrepared() throws Exception {
        // TODO
    }

    @Test
    public void testInsert() throws Exception {
        {
            String sql = "insert into testtbl(k1, k2) values (1, 'muga')";
            Statement stat = s(sql);

            assertTrue(stat instanceof Insert);
            Insert insert = (Insert) stat;
            assertEquals("testtbl", insert.getTable().getName());
            List<Column> columns = insert.getColumns();
            assertEquals(2, columns.size());
            assertEquals("k1", columns.get(0).getColumnName());
            assertEquals("k2", columns.get(1).getColumnName());
            ExpressionList exprs = (ExpressionList) insert.getItemsList();
            List<Expression> es = exprs.getExpressions();
            assertEquals(2, es.size());
            assertEquals(1, ((LongValue) es.get(0)).getValue());
            assertEquals("muga", ((StringValue) es.get(1)).getValue());
        }
        {
            String sql = "Insert into testtbl(k1, k2) values (1, 'muga')";
            Statement stat = s(sql);

            assertTrue(stat instanceof Insert);
            Insert insert = (Insert) stat;
            assertEquals("testtbl", insert.getTable().getName());
            List<Column> columns = insert.getColumns();
            assertEquals(2, columns.size());
            assertEquals("k1", columns.get(0).getColumnName());
            assertEquals("k2", columns.get(1).getColumnName());
            ExpressionList exprs = (ExpressionList) insert.getItemsList();
            List<Expression> es = exprs.getExpressions();
            assertEquals(2, es.size());
            assertEquals(1, ((LongValue) es.get(0)).getValue());
            assertEquals("muga", ((StringValue) es.get(1)).getValue());
        }
        {
            String sql = "Insert intttt testtbl(k1, k2) values (1, 'muga')";
            try {
                s(sql);
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof ParseException);
            }
        }
        {
            String sql = "Insert into testtbl(k1, k2) vvvvlues (1, 'muga')";
            try {
                s(sql);
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof ParseException);
            }
        }
    }

    @Test
    public void testInsertPrepared() throws Exception {
        //insert into table02 (k1, k2, k3) values (?, ?, 'nishizawa')
        // TODO
    }

    @Test
    public void testCreateTable() throws Exception {
        //create table table01(c0 varchar(255), c1 int)
        {
            String sql = "create table testtbl(p0 varchar(255), p1 int)";
            Statement stat = s(sql);

            assertTrue(stat instanceof CreateTable);
            CreateTable ctable = (CreateTable) stat;
            assertEquals("testtbl", ctable.getTable().getName());
            List<ColumnDefinition> cdefs = ctable.getColumnDefinitions();
            assertEquals(2, cdefs.size());
            ColumnDefinition cdef0 = cdefs.get(0);
            assertEquals("p0", cdef0.getColumnName());
            assertEquals("varchar", cdef0.getColDataType().getDataType());
            ColumnDefinition cdef1 = cdefs.get(1);
            assertEquals("p1", cdef1.getColumnName());
            assertEquals("int", cdef1.getColDataType().getDataType());
        }
    }

    @Test
    public void testCreateTablePrepared() throws Exception {
        // TODO is it needed??
    }

    @Test
    public void testDrop() throws Exception {
        {
            String sql = "drop table testtbl";
            Statement stat = s(sql);

            assertTrue(stat instanceof Drop);
            Drop drop = (Drop) stat;
            assertEquals("testtbl", drop.getName());
            assertEquals("table", drop.getType());
        }
        {
            String sql = "Drop table testtbl";
            Statement stat = s(sql);

            assertTrue(stat instanceof Drop);
            Drop drop = (Drop) stat;
            assertEquals("testtbl", drop.getName());
            assertEquals("table", drop.getType());
        }
    }

    @Test
    public void testDropPrepared() throws Exception {
        // TODO is it needed??
    }

    @Test
    public void testShow() throws Exception {
        {
            String sql = "show foo";
            try {
                s(sql);
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
                s(sql);
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
