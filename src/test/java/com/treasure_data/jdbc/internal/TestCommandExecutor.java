package com.treasure_data.jdbc.internal;

import java.util.Properties;

import org.hsqldb.result.ResultConstants;
import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.model.Database;

public class TestCommandExecutor {

    @Test @Ignore
    public void select01() throws Exception {
        String sql = "select count(*) from mugatbl order by c1 desc";
        CommandExecutor exec = new CommandExecutor(new NullClientAdaptor());
        exec.execute(ResultConstants.EXECDIRECT, sql);
    }

    @Test @Ignore // TODO #MN
    public void select02() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        TreasureDataClient client = new TreasureDataClient(props);
        TreasureDataClientAdaptor clientAdaptor =
            new TreasureDataClientAdaptor(client, new Database("mugadb"));
        CommandExecutor exec = new CommandExecutor(clientAdaptor);

        String sql = "create table table01(c0 varchar(255), c1 int)";
        exec.execute(ResultConstants.EXECDIRECT, sql);
    }

    @Test @Ignore
    public void createTable01() throws Exception {
        String sql = "create table table01(c0 varchar(255), c1 int)";
        CommandExecutor exec = new CommandExecutor(new NullClientAdaptor());
        exec.execute(ResultConstants.EXECDIRECT, sql);
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
        exec.execute(ResultConstants.EXECDIRECT, sql);
    }

    @Test @Ignore
    public void insert01() throws Exception {
        String sql = "insert into table02 (k1, k2, k3) values (2, 'muga', 'nishizawa')";
        CommandExecutor exec = new CommandExecutor(new NullClientAdaptor());
        exec.execute(ResultConstants.EXECDIRECT, sql);
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
            String sql0 = String.format(sql,
                    "col1", "col2", "" + i, "muga:" + i);
            exec.execute(ResultConstants.EXECDIRECT, sql0);
        }

        clientAdaptor.flush();
    }

}
