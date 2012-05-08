package com.treasure_data.jdbc.internal;

import java.io.ByteArrayInputStream;
import java.util.Iterator;
import java.util.List;

import org.hsqldb.HsqlException;
import org.hsqldb.Scanner;
import org.hsqldb.SessionContext;
import org.hsqldb.Statement;
import org.hsqldb.StatementTypes;
//import org.hsqldb.TreasureDataParser;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.java.JavaSystem;
import org.hsqldb.persist.HsqlDatabaseProperties;
import org.hsqldb.result.ResultConstants;
import org.hsqldb.result.ResultLob;
import org.hsqldb.store.ValuePool;

import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.jdbc.compiler.expr.ops.ItemsList;
import com.treasure_data.jdbc.compiler.parser.CCSQLParser;
import com.treasure_data.jdbc.compiler.parser.ParseException;
import com.treasure_data.jdbc.compiler.schema.Column;
import com.treasure_data.jdbc.compiler.schema.Table;
import com.treasure_data.jdbc.compiler.stat.CreateTable;
import com.treasure_data.jdbc.compiler.stat.Insert;
import com.treasure_data.jdbc.compiler.stat.Select;

/**
 * @see org.hsqldb.Session
 * @see org.hsqldb.SessionInterface
 */
public class CommandExecutor {
    private TreasureDataClient client;

    public CommandExecutor(TreasureDataClient client) {
        this.client = client;
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

    public synchronized Result execute(Result cmd) {
        switch (cmd.mode) {
        case ResultConstants.LARGE_OBJECT_OP:
        case ResultConstants.EXECUTE:
        case ResultConstants.BATCHEXECUTE:
            throw new UnsupportedOperationException();

        case ResultConstants.EXECDIRECT:
            Result result = executeDirectStatement(cmd);
            result = performPostExecute(cmd, result);
            return result;

        case ResultConstants.BATCHEXECDIRECT:
        case ResultConstants.PREPARE:
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
            throw new UnsupportedOperationException();
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

    public Result executeDirectStatement(Result cmd) {
        // TODO
        return null;
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
        System.out.println("call executeCompiledSelect");
        return null;
    }

    public org.hsqldb.result.Result executeCompiledInsert(Insert stat,
            Object[] pvals) {
        List<Column> cols = stat.getColumns();
        ItemsList items = stat.getItemsList();
        Table table = stat.getTable();
        System.out.println("call executeCompiledInsert");
        return null;
    }

    public org.hsqldb.result.Result executeCompiledCreateTable(CreateTable stat,
            Object[] pvals) {
        System.out.println("call executeCompiledCreateTable");
        return null;
    }

    private org.hsqldb.result.Result performPostExecute(
            org.hsqldb.result.Result cmd, org.hsqldb.result.Result result) {
        return result; // TODO
    }

    private Result performPostExecute(Result cmd, Result result) {
        return result; // TODO
    }

}
