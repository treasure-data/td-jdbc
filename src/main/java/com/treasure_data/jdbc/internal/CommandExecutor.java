package com.treasure_data.jdbc.internal;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.hsqldb.result.ResultConstants;
import org.hsqldb.store.ValuePool;

import com.treasure_data.jdbc.compiler.expr.DateValue;
import com.treasure_data.jdbc.compiler.expr.DoubleValue;
import com.treasure_data.jdbc.compiler.expr.Expression;
import com.treasure_data.jdbc.compiler.expr.LongValue;
import com.treasure_data.jdbc.compiler.expr.NullValue;
import com.treasure_data.jdbc.compiler.expr.StringValue;
import com.treasure_data.jdbc.compiler.expr.TimeValue;
import com.treasure_data.jdbc.compiler.expr.ops.ExpressionList;
import com.treasure_data.jdbc.compiler.expr.ops.ItemsList;
import com.treasure_data.jdbc.compiler.parser.CCSQLParser;
import com.treasure_data.jdbc.compiler.parser.ParseException;
import com.treasure_data.jdbc.compiler.schema.Column;
import com.treasure_data.jdbc.compiler.schema.Table;
import com.treasure_data.jdbc.compiler.stat.ColumnDefinition;
import com.treasure_data.jdbc.compiler.stat.CreateTable;
import com.treasure_data.jdbc.compiler.stat.Index;
import com.treasure_data.jdbc.compiler.stat.Insert;
import com.treasure_data.jdbc.compiler.stat.Select;

/**
 * @see org.hsqldb.Session
 * @see org.hsqldb.SessionInterface
 */
public class CommandExecutor {
    private ClientAdaptor clientAdaptor;

    public CommandExecutor(ClientAdaptor clientAdaptor) {
        this.clientAdaptor = clientAdaptor;
    }

    public synchronized org.hsqldb.result.Result execute(org.hsqldb.result.Result cmd) {
        switch (cmd.mode) {
        case ResultConstants.LARGE_OBJECT_OP:
        case ResultConstants.EXECUTE:
        case ResultConstants.BATCHEXECUTE:
            return org.hsqldb.result.Result.newErrorResult(
                    new UnsupportedOperationException());

        case ResultConstants.EXECDIRECT:
            org.hsqldb.result.Result result = executeDirectStatement(cmd);
            result = performPostExecute(cmd, result);
            return result;

        case ResultConstants.BATCHEXECDIRECT:
        case ResultConstants.PREPARE: // TODO
        case ResultConstants.CLOSE_RESULT:
        case ResultConstants.UPDATE_RESULT:
        case ResultConstants.FREESTMT:
        case ResultConstants.GETSESSIONATTR:
        case ResultConstants.SETSESSIONATTR:
        case ResultConstants.ENDTRAN:
        case ResultConstants.SETCONNECTATTR:
        case ResultConstants.REQUESTDATA:
        case ResultConstants.DISCONNECT:
        default:
            return org.hsqldb.result.Result.newErrorResult(
                    new UnsupportedOperationException());
        }
    }

    public org.hsqldb.result.Result executeDirectStatement(
            org.hsqldb.result.Result cmd) {
        String sql = cmd.getMainString();

        com.treasure_data.jdbc.compiler.stat.Statement stat = null;
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(sql.getBytes());
            CCSQLParser p = new CCSQLParser(in);
            stat = p.Statement();
        } catch (ParseException e) {
            return org.hsqldb.result.Result.newErrorResult(e);
        }
        org.hsqldb.result.Result result =
            executeCompiledStatement(stat, ValuePool.emptyObjectArray);

        return result;
    }

    public org.hsqldb.result.Result executeCompiledStatement(
            com.treasure_data.jdbc.compiler.stat.Statement stat,
            Object[] pvals) {
        if (stat instanceof Insert) {
            return executeCompiledInsert((Insert) stat, pvals);
        } else if (stat instanceof CreateTable) {
            return executeCompiledCreateTable((CreateTable) stat, pvals);
        } else if (stat instanceof Select) {
            return executeCompiledSelect((Select) stat, pvals);
        } else {
            return org.hsqldb.result.Result.newErrorResult(
                    new UnsupportedOperationException(),
                    stat.toString());
        }
    }

    public org.hsqldb.result.Result executeCompiledSelect(Select stat,
            Object[] pvals) {
        String sql = stat.toString();
        clientAdaptor.select(sql);
        return null;
    }

    public org.hsqldb.result.Result executeCompiledInsert(Insert stat,
            Object[] pvals) {
        /**
         * SQL:
         * insert into table02 (k1, k2, k3) values (2, 'muga', 'nishizawa')
         *
         * ret:
         * table => table02
         * cols  => [k1, k2, k3]
         * items => (2, 'muga', 'nishizawa')
         */

        Table table = stat.getTable();
        // table validation
        if (table == null
                || table.getName() == null
                || table.getName().isEmpty()) {
            return org.hsqldb.result.Result.newErrorResult(
                    new UnsupportedOperationException(),
                    stat.toString());
        }

        // columns validation
        List<Column> cols = stat.getColumns();
        if (cols == null || cols.size() <= 0) {
            return org.hsqldb.result.Result.newErrorResult(
                    new UnsupportedOperationException(),
                    stat.toString());
        }

        // items validation
        List<Expression> exprs;
        {
            ItemsList items = stat.getItemsList();
            if (items == null) {
                return org.hsqldb.result.Result.newErrorResult(
                        new UnsupportedOperationException(),
                        stat.toString());
            }
            try {
                exprs = ((ExpressionList) items).getExpressions();
            } catch (Throwable t) {
                return org.hsqldb.result.Result.newErrorResult(
                        new UnsupportedOperationException(),
                        stat.toString());
            }
        }

        // other validations
        if (cols.size() != exprs.size()) {
            return org.hsqldb.result.Result.newErrorResult(
                    new UnsupportedOperationException(),
                    stat.toString());
        }

        try {
            Map<String, Object> record = new HashMap<String, Object>();
            Iterator<Column> col_iter = cols.iterator();
            Iterator<Expression> expr_iter = exprs.iterator();
            while (col_iter.hasNext()) {
                Column col = col_iter.next();
                Expression expr = expr_iter.next();
                record.put(col.getColumnName(), toValue(expr));
            }
            System.out.println("table : " + table.getName());
            System.out.println("record: " + record);
            clientAdaptor.insertData(table.getName(), record);
        } catch (Exception e) {
            return org.hsqldb.result.Result.newErrorResult(
                    new UnsupportedOperationException(),
                    stat.toString());
        }

        return null; // TODO
    }

    public org.hsqldb.result.Result executeCompiledCreateTable(CreateTable stat,
            Object[] pvals) {
        /**
         * SQL:
         * create table table01(c0 varchar(255), c1 int)
         *
         * ret:
         * table => table02
         */

        // table validation
        Table table = stat.getTable();
        if (table == null
                || table.getName() == null
                || table.getName().isEmpty()) {
            return org.hsqldb.result.Result.newErrorResult(
                    new UnsupportedOperationException(),
                    stat.toString());
        }

        // column definition validation
        List<ColumnDefinition> def = stat.getColumnDefinitions();
        if (def == null || def.size() == 0) {
            return org.hsqldb.result.Result.newErrorResult(
                    new UnsupportedOperationException(),
                    stat.toString());
        }

        // this variable is not used
        List<Index> indexes = stat.getIndexes();

        try {
            clientAdaptor.createTable(table.getName());
        } catch (Exception e) {
            return org.hsqldb.result.Result.newErrorResult(
                    new UnsupportedOperationException(),
                    stat.toString());
        }

        return null;
    }

    private org.hsqldb.result.Result performPostExecute(
            org.hsqldb.result.Result cmd, org.hsqldb.result.Result result) {
        return result; // TODO
    }

    private static Object toValue(Expression expr) {
        if (expr instanceof DateValue) {
            DateValue v = (DateValue) expr;
            return v.getValue().getTime() / 1000;
        } else if (expr instanceof DoubleValue) {
            DoubleValue v = (DoubleValue) expr;
            return v.getValue();
        } else if (expr instanceof LongValue) {
            LongValue v = (LongValue) expr;
            return v.getValue();
        } else if (expr instanceof NullValue) {
            return null;
        } else if (expr instanceof StringValue) {
            StringValue v = (StringValue) expr;
            return v.getValue();
        } else if (expr instanceof TimeValue) {
            TimeValue v = (TimeValue) expr;
            return v.getValue().getTime() / 1000;
        } else {
            return org.hsqldb.result.Result.newErrorResult(
                    new UnsupportedOperationException(),
                    expr.toString());
        }
    }
}
