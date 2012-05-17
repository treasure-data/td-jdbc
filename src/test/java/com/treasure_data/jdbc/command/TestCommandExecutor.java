package com.treasure_data.jdbc.command;

import java.util.Properties;

import org.hsqldb.result.ResultConstants;
import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.jdbc.command.CommandExecutor;
import com.treasure_data.jdbc.command.NullClientAPI;
import com.treasure_data.jdbc.command.TDClientAPI;
import com.treasure_data.model.Database;

public class TestCommandExecutor {

    @Test @Ignore
    public void select01() throws Exception {
        CommandExecutor exec = new CommandExecutor(new NullClientAPI());
        CommandContext w = new CommandContext();
        w.mode = ResultConstants.EXECDIRECT;
        w.sql = "select count(*) from mugatbl order by c1 desc";
        exec.execute(w);
    }

    @Test @Ignore
    public void select02() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        TreasureDataClient client = new TreasureDataClient(props);
        TDClientAPI clientAdaptor =
            new TDClientAPI(client, new Database("mugadb"));
        CommandExecutor exec = new CommandExecutor(clientAdaptor);

        CommandContext w = new CommandContext();
        w.mode = ResultConstants.EXECDIRECT;
        w.sql = "create table table01(c0 varchar(255), c1 int)";
        exec.execute(w);
    }

    @Test @Ignore
    public void createTable01() throws Exception {
        CommandExecutor exec = new CommandExecutor(new NullClientAPI());
        CommandContext w = new CommandContext();
        w.mode = ResultConstants.EXECDIRECT;
        w.sql = "create table table01(c0 varchar(255), c1 int)";
        exec.execute(w);
    }

    @Test @Ignore
    public void createTable02() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        TreasureDataClient client = new TreasureDataClient(props);
        TDClientAPI clientAdaptor =
            new TDClientAPI(client, new Database("mugadb"));
        CommandExecutor exec = new CommandExecutor(clientAdaptor);

        CommandContext w = new CommandContext();
        w.mode = ResultConstants.EXECDIRECT;
        w.sql = "create table table01(c0 varchar(255), c1 int)";
        exec.execute(w);
    }

    @Test @Ignore
    public void insert01() throws Exception {
        CommandExecutor exec = new CommandExecutor(new NullClientAPI());
        CommandContext w = new CommandContext();
        w.mode = ResultConstants.EXECDIRECT;
        w.sql = "insert into table02 (k1, k2, k3) values (2, 'muga', 'nishizawa')";
        exec.execute(w);
    }

    @Test @Ignore
    public void insert02() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        TreasureDataClient client = new TreasureDataClient(props);
        TDClientAPI clientAdaptor =
            new TDClientAPI(client, new Database("mugadb"));
        CommandExecutor exec = new CommandExecutor(clientAdaptor);

        String sql = "insert into table01 (%s, %s) values (%s, '%s')";
        for (int i = 0; i < 50; i++) {
            CommandContext w = new CommandContext();
            w.mode = ResultConstants.EXECDIRECT;
            w.sql = String.format(sql, "col1", "col2", "" + i, "muga:" + i);
            exec.execute(w);
        }

        clientAdaptor.flush();
    }

}
