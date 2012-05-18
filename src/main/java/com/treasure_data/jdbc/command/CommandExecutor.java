package com.treasure_data.jdbc.command;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.hsqldb.result.ResultConstants;

import com.treasure_data.client.ClientException;
import com.treasure_data.jdbc.compiler.expr.DateValue;
import com.treasure_data.jdbc.compiler.expr.DoubleValue;
import com.treasure_data.jdbc.compiler.expr.Expression;
import com.treasure_data.jdbc.compiler.expr.JdbcParameter;
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
import com.treasure_data.jdbc.compiler.stat.Drop;
import com.treasure_data.jdbc.compiler.stat.Index;
import com.treasure_data.jdbc.compiler.stat.Insert;
import com.treasure_data.jdbc.compiler.stat.Select;
import com.treasure_data.jdbc.compiler.stat.Statement;

/**
 * @see org.hsqldb.Session
 * @see org.hsqldb.SessionInterface
 */
public class CommandExecutor {
    private ClientAPI api;

    public CommandExecutor(ClientAPI api) {
        this.api = api;
    }

    public ClientAPI getAPI() {
        return api;
    }

    public synchronized void execute(CommandContext context)
            throws SQLException {
        switch (context.mode) {
        case ResultConstants.EXECDIRECT:
            executeDirect(context);
            break;

        case ResultConstants.EXECUTE:
        case ResultConstants.BATCHEXECUTE:
            throw new SQLException("invalid mode: " + context.mode);

        case ResultConstants.PREPARE:
            executePrepare(context);
            break;

        case ResultConstants.LARGE_OBJECT_OP:
        case ResultConstants.BATCHEXECDIRECT:
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
            throw new SQLException("invalid mode: " + context.mode);
        }
    }

    public void executeDirect(CommandContext context) throws SQLException {
        try {
            String sql = context.sql;
            InputStream in = new ByteArrayInputStream(sql.getBytes());
            CCSQLParser p = new CCSQLParser(in);
            context.compiledSql = p.Statement();
            validateStatement(context);
            extractJdbcParameters(context);
            if (context.paramList.size() != 0) {
                throw new ParseException("sql includes some jdbcParameters");
            }
            executeCompiledStatement(context);
        } catch (ParseException e) {
            throw new SQLException(e);
        }
    }

    public void executePrepare(CommandContext context) throws SQLException {
        if (context.compiledSql == null) {
            try {
                String sql = context.sql;
                InputStream in = new ByteArrayInputStream(sql.getBytes());
                CCSQLParser p = new CCSQLParser(in);
                context.compiledSql = p.Statement();
                validateStatement(context);
                extractJdbcParameters(context);
            } catch (ParseException e) {
                throw new SQLException(e);
            }
        } else {
            executeCompiledPreparedStatement(context);
        }
    }

    public void validateStatement(CommandContext context)
            throws ParseException {
        Statement stat = context.compiledSql;
        if (stat == null) {
            throw new ParseException("stat is null");
        }

        if (stat instanceof Insert) {
            validateStatement(context, (Insert) stat);
        } else if (stat instanceof CreateTable) {
            validateStatement(context, (CreateTable) stat);
        } else if (stat instanceof Drop) {
            validateStatement(context, (Drop) stat);
        } else if (stat instanceof Select) {
            validateStatement(context, (Select) stat);
        } else {
            throw new ParseException("unsupported statement: " + stat);
        }
    }

    public void extractJdbcParameters(CommandContext context)
            throws ParseException {
        context.paramList = new ArrayList<String>();
        Statement stat = context.compiledSql;
        if (stat instanceof Insert) {
            extractJdbcParameters(context, (Insert) stat);
        } else if (stat instanceof CreateTable) {
            extractJdbcParameters(context, (CreateTable) stat);
        } else if (stat instanceof Drop) {
            extractJdbcParameters(context, (Drop) stat);
        } else if (stat instanceof Select) {
            extractJdbcParameters(context, (Select) stat);
        } else {
            throw new ParseException("unsupported statement: " + stat);
        }
    }

    public void executeCompiledStatement(CommandContext context)
            throws SQLException {
        Statement stat = context.compiledSql;
        if (stat instanceof Insert) {
            executeCompiledStatement(context, (Insert) stat);
        } else if (stat instanceof CreateTable) {
            executeCompiledStatement(context, (CreateTable) stat);
        } else if (stat instanceof Drop) {
            executeCompiledStatement(context, (Drop) stat);
        } else if (stat instanceof Select) {
            executeCompiledStatement(context, (Select) stat);
        } else {
            throw new SQLException("unsupported statement: " + stat);
        }
    }

    public void executeCompiledPreparedStatement(CommandContext context)
            throws SQLException {
        Statement stat = context.compiledSql;
        if (stat instanceof Insert) {
            executeCompiledPreparedStatement(context, (Insert) stat);
        } else if (stat instanceof CreateTable) {
            executeCompiledPreparedStatement(context, (CreateTable) stat);
        } else if (stat instanceof Drop) {
            executeCompiledPreparedStatement(context, (Drop) stat);
        } else if (stat instanceof Select) {
            executeCompiledPreparedStatement(context, (Select) stat);
        } else {
            throw new SQLException("unsupported statement: " + stat);
        }
    }

    public void validateStatement(CommandContext context, Select stat)
            throws ParseException {
        // ignore
    }

    public void executeCompiledStatement(CommandContext context,
            Select stat) throws SQLException {
        String sql = stat.toString();
        try {
            context.resultSet = api.select(sql);
        } catch (ClientException e) {
            throw new SQLException(e);
        }
    }

    public void executeCompiledPreparedStatement(CommandContext context,
            Select stat) throws SQLException {
        executeCompiledStatement(context, stat);
    }

    public void extractJdbcParameters(CommandContext context, Select stat) {
        // ignore
    }

    public void validateStatement(CommandContext context, Insert stat)
            throws ParseException {
        Table table = stat.getTable();
        // table validation
        if (table == null
                || table.getName() == null
                || table.getName().isEmpty()) {
            throw new ParseException("invalid table name: " + table);
        }

        // columns validation
        List<Column> cols = stat.getColumns();
        if (cols == null || cols.size() <= 0) {
            throw new ParseException("invalid columns: " +  cols);
        }

        // items validation
        List<Expression> exprs;
        {
            ItemsList items = stat.getItemsList();
            if (items == null) {
                throw new ParseException("invalid item list: " + items);
            }
            try {
                exprs = ((ExpressionList) items).getExpressions();
            } catch (Throwable t) {
                throw new ParseException("unsupported expressions");
            }
        }

        // other validations
        if (cols.size() != exprs.size()) {
            throw new ParseException("invalid columns or expressions");
        }
    }

    public void executeCompiledStatement(CommandContext context, Insert stat)
            throws SQLException {
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
        List<Column> cols = stat.getColumns();
        List<Expression> exprs = ((ExpressionList) stat.getItemsList()).getExpressions();

        try {
            Map<String, Object> record = new HashMap<String, Object>();
            Iterator<Column> col_iter = cols.iterator();
            Iterator<Expression> expr_iter = exprs.iterator();
            while (col_iter.hasNext()) {
                Column col = col_iter.next();
                Expression expr = expr_iter.next();
                record.put(col.getColumnName(), toValue(expr));
            }
            api.insert(table.getName(), record);
        } catch (Exception e) {
            throw new UnsupportedOperationException();
        }
    }

    public void executeCompiledPreparedStatement(CommandContext context, Insert stat) {
        /**
         * SQL:
         * insert into table02 (k1, k2, k3) values (?, ?, 'nishizawa')
         *
         * ret:
         * table => table02
         * cols  => [k1, k2, k3]
         * items => (?, ?, 'nishizawa')
         */

        Table table = stat.getTable();
        List<Column> cols = stat.getColumns();
        List<Expression> exprs = ((ExpressionList) stat.getItemsList()).getExpressions();
        List<String> paramList = context.paramList;
        Map<Integer, Object> params = context.params;

        try {
            Map<String, Object> record = new HashMap<String, Object>();
            Iterator<Column> col_iter = cols.iterator();
            Iterator<Expression> expr_iter = exprs.iterator();
            while (col_iter.hasNext()) {
                Column col = col_iter.next();
                Expression expr = expr_iter.next();
                String colName = col.getColumnName();
                int i = getIndex(paramList, colName);
                if (i >= 0) {
                    record.put(colName, params.get(new Integer(i + 1)));
                } else {
                    record.put(colName, toValue(expr));
                }
            }
            api.insert(table.getName(), record);
        } catch (Exception e) {
            throw new UnsupportedOperationException();
        }
    }

    public void extractJdbcParameters(CommandContext context, Insert stat) {
        List<Column> cols = stat.getColumns();
        List<Expression> exprs = ((ExpressionList) stat.getItemsList()).getExpressions();
        int len = cols.size();
        for (int i = 0; i < len; i++) {
            Expression expr = exprs.get(i);
            if (! (expr instanceof JdbcParameter)) {
                continue;
            }

            String colName = cols.get(i).getColumnName();
            context.paramList.add(colName);
        }
    }

    public void validateStatement(CommandContext context, CreateTable stat)
            throws ParseException {
        // table validation
        Table table = stat.getTable();
        if (table == null
                || table.getName() == null
                || table.getName().isEmpty()) {
            throw new ParseException("invalid table name: " + table);
                    }

        // column definition validation
        List<ColumnDefinition> def = stat.getColumnDefinitions();
        if (def == null || def.size() == 0) {
            throw new ParseException("invalid column definitions: " + def);
        }

        // this variable is not used
        @SuppressWarnings("unused")
        List<Index> indexes = stat.getIndexes();
    }

    public void executeCompiledStatement(CommandContext context, CreateTable stat) {
        /**
         * SQL:
         * create table table01(c0 varchar(255), c1 int)
         *
         * ret:
         * table => table02
         */

        Table table = stat.getTable();
        List<ColumnDefinition> def = stat.getColumnDefinitions();
        @SuppressWarnings("unused")
        List<Index> indexes = stat.getIndexes();

        try {
            api.create(table.getName());
        } catch (Exception e) {
            throw new UnsupportedOperationException();
        }
    }

    public void executeCompiledPreparedStatement(CommandContext context,
            CreateTable stat) throws SQLException {
        executeCompiledStatement(context, stat);
    }

    public void extractJdbcParameters(CommandContext context, CreateTable stat) {
        // ignore
    }

    public void validateStatement(CommandContext context, Drop stat)
            throws ParseException {
        String tableName = stat.getName();
        if (tableName == null || tableName.isEmpty()) {
            throw new ParseException("invalid table name: " + tableName);
        }

        @SuppressWarnings("unused")
        List<String> params = stat.getParameters();

        String type = stat.getType();
        if (! (type.equals("table"))) {
            throw new ParseException("unsupported type: " + type);
        }
    }

    public void executeCompiledStatement(CommandContext context, Drop stat) {
        /**
         * SQL:
         * drop table table02
         *
         * ret:
         * table => table02
         */

        String tableName = stat.getName();
        @SuppressWarnings("unused")
        List<String> params = stat.getParameters();
        String type = stat.getType();

        try {
            api.drop(tableName);
        } catch (ClientException e) {
            throw new UnsupportedOperationException();
        }
    }

    public void executeCompiledPreparedStatement(CommandContext context,
            Drop stat) throws SQLException {
        executeCompiledStatement(context, stat);
    }

    public void extractJdbcParameters(CommandContext context, Drop stat) {
        // ignore
    }

    private static int getIndex(List<String> list, String data) {
        for (int i = 0; i < list.size(); i++) {
            String d = list.get(i);
            if (d.equals(data)) {
                return i;
            }
        }
        return -1;
    }

    private static Object toValue(Expression expr) throws ParseException {
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
            throw new ParseException(
                    String.format("Type of value is not supported: %s", expr));
        }
    }
}
