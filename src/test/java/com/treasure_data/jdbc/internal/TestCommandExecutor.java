package com.treasure_data.jdbc.internal;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.hsqldb.StatementTypes;
import org.hsqldb.result.Result;
import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.jdbc.JDBCAbstractStatement;
import com.treasure_data.logger.TreasureDataLogger;
import com.treasure_data.model.Database;

public class TestCommandExecutor {

    @Test @Ignore
    public void select01() throws Exception {
        String sql = "select count(*) from mugatbl order by c1 desc";
        int maxRows = 100;
        int fetchSize = 64 * 1024;
        int queryTimeout = 0;
        int rsProperties = 0;
        CommandExecutor exec = new CommandExecutor(new NullClientAdaptor());
        Result out = Result.newExecuteDirectRequest();
        out.setPrepareOrExecuteProperties(sql, maxRows, fetchSize,
                StatementTypes.RETURN_RESULT,
                queryTimeout, rsProperties,
                JDBCAbstractStatement.NO_GENERATED_KEYS, null, null);
        Result in = exec.execute(out);
    }

    @Test @Ignore
    public void createTable01() throws Exception {
        String sql = "create table table01(c0 varchar(255), c1 int)";
        //String sql = "insert into table02 (k1, k2, k3) values (2, 'muga', 'nishizawa')";
        //String sql = "insert into table02 values (2, 'muga', 'nishizawa')";
        int maxRows = 100;
        int fetchSize = 64 * 1024;
        int queryTimeout = 0;
        int rsProperties = 0;
        CommandExecutor exec = new CommandExecutor(new NullClientAdaptor());
        Result out = Result.newExecuteDirectRequest();
        out.setPrepareOrExecuteProperties(sql, maxRows, fetchSize,
                StatementTypes.RETURN_RESULT,
                queryTimeout, rsProperties,
                JDBCAbstractStatement.NO_GENERATED_KEYS, null, null);
        Result in = exec.execute(out);
    }

    @Test @Ignore
    public void createTable02() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        TreasureDataClient client = new TreasureDataClient(props);
        TreasureDataClientAdaptor clientAdaptor =
            new TreasureDataClientAdaptor(client, new Database("mugadb"));
        CommandExecutor exec = new CommandExecutor(clientAdaptor);

        String sql = "create table table01(c0 varchar(255), c1 int)";
        //String sql = "insert into table02 (k1, k2, k3) values (2, 'muga', 'nishizawa')";
        //String sql = "insert into table02 values (2, 'muga', 'nishizawa')";
        int maxRows = 100;
        int fetchSize = 64 * 1024;
        int queryTimeout = 0;
        int rsProperties = 0;
        Result out = Result.newExecuteDirectRequest();
        out.setPrepareOrExecuteProperties(sql, maxRows, fetchSize,
                StatementTypes.RETURN_RESULT,
                queryTimeout, rsProperties,
                JDBCAbstractStatement.NO_GENERATED_KEYS, null, null);
        Result in = exec.execute(out);
    }

    @Test @Ignore
    public void insert01() throws Exception {
        //String sql = "create table table01(c0 varchar(255), c1 int)";
        String sql = "insert into table02 (k1, k2, k3) values (2, 'muga', 'nishizawa')";
        //String sql = "insert into table02 values (2, 'muga', 'nishizawa')";
        int maxRows = 100;
        int fetchSize = 64 * 1024;
        int queryTimeout = 0;
        int rsProperties = 0;
        CommandExecutor exec = new CommandExecutor(new NullClientAdaptor());
        Result out = Result.newExecuteDirectRequest();
        out.setPrepareOrExecuteProperties(sql, maxRows, fetchSize,
                StatementTypes.RETURN_RESULT,
                queryTimeout, rsProperties,
                JDBCAbstractStatement.NO_GENERATED_KEYS, null, null);
        Result in = exec.execute(out);
    }

    @Test @Ignore
    public void insert02() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        TreasureDataClient client = new TreasureDataClient(props);
        TreasureDataClientAdaptor clientAdaptor =
            new TreasureDataClientAdaptor(client, new Database("mugadb"));
        CommandExecutor exec = new CommandExecutor(clientAdaptor);

        String sql = "insert into table01 (%s, %s) values (%s, '%s')";
        for (int i = 0; i < 50; i++) {
            int maxRows = 100;
            int fetchSize = 64 * 1024;
            int queryTimeout = 0;
            int rsProperties = 0;
            Result out = Result.newExecuteDirectRequest();
            String sql0 = String.format(sql,
                    "col1", "col2", "" + i, "muga:" + i);
            out.setPrepareOrExecuteProperties(sql0, maxRows, fetchSize,
                    StatementTypes.RETURN_RESULT,
                    queryTimeout, rsProperties,
                    JDBCAbstractStatement.NO_GENERATED_KEYS, null, null);
            Result in = exec.execute(out);
        }

        clientAdaptor.flush();
    }

}
