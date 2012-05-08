package com.treasure_data.jdbc.internal;

import org.hsqldb.StatementTypes;
import org.hsqldb.result.Result;
import org.junit.Test;

import com.treasure_data.jdbc.JDBCAbstractStatement;

public class TestCommandExecutor {

    @Test
    public static void main(String[] args) throws Exception {
        String sql = "create table tabl01(c0 varchar(255), c1 int)";
        int maxRows = 100;
        int fetchSize = 64 * 1024;
        int queryTimeout = 0;
        int rsProperties = 0;
        CommandExecutor exec = new CommandExecutor(null);
        Result out = Result.newExecuteDirectRequest();
        out.setPrepareOrExecuteProperties(sql, maxRows, fetchSize,
                StatementTypes.RETURN_RESULT,
                queryTimeout, rsProperties,
                JDBCAbstractStatement.NO_GENERATED_KEYS, null, null);
        Result in = exec.execute(out);

    }
}
