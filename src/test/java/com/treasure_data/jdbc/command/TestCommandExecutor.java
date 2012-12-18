package com.treasure_data.jdbc.command;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;
import org.msgpack.unpacker.Unpacker;

import com.treasure_data.client.ClientException;
import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.jdbc.Constants;
import com.treasure_data.jdbc.TDResultSetBase;
import com.treasure_data.jdbc.command.CommandExecutor;
import com.treasure_data.jdbc.command.TDClientAPI;
import com.treasure_data.model.Database;
import com.treasure_data.model.DatabaseSummary;
import com.treasure_data.model.Job;
import com.treasure_data.model.JobSummary;
import com.treasure_data.model.TableSummary;

public class TestCommandExecutor {

    /*
     * throw an exception by invalid context.mode
     * invalid mode means number without EXECDIRECT and PREPARE
     */
    @Test
    public void testExecute01() throws Exception {
        ClientAPI clientApi = new NullClientAPI();
        CommandExecutor exec = new CommandExecutor(clientApi);

        CommandContext context = new CommandContext();
        context.mode = Constants.CHANGE_SET; // without EXECDIRECT and PREPARE
        try {
            exec.execute(context);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof SQLException);
        }
    }

    /*
     * throw an exception by invalid context.sql
     */
    @Test
    public void testExecute02() throws Exception {
        ClientAPI clientApi = new NullClientAPI();
        CommandExecutor exec = new CommandExecutor(clientApi);

        CommandContext context = new CommandContext();
        context.mode = Constants.EXECDIRECT;
        context.sql = "illigal statement";
        try {
            exec.execute(context);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof SQLException);
        }
    }

    /*
     * throw an exception by invalid context.sql
     * invalid sql means sql statement without insert/create/drop/select
     */
    @Test
    public void testExecute03() throws Exception {
        ClientAPI clientApi = new NullClientAPI();
        CommandExecutor exec = new CommandExecutor(clientApi);

        CommandContext context = new CommandContext();
        context.mode = Constants.EXECDIRECT;
        context.sql = "update foo set k='muga' where id = 100";
        try {
            exec.execute(context);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof SQLException);
        }
    }

    /*
     * throw an exception by invalid context.sql
     * here invalid sql means a sql statement that includes jdbc parameters like '?'.
     */
    @Test
    public void testExecute04() throws Exception {
        ClientAPI clientApi = new NullClientAPI();
        CommandExecutor exec = new CommandExecutor(clientApi);

        CommandContext context = new CommandContext();
        context.mode = Constants.EXECDIRECT;
        context.sql = "insert into foo (k1, k2) values (?, 'muga')";
        try {
            exec.execute(context);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof SQLException);
        }
    }

    @Test
    public void testExecute05() throws Exception {
        ClientAPI clientApi = new ClientAPI() {
            public List<DatabaseSummary> showDatabases() throws ClientException {
                return null;
            }

            public List<TableSummary> showTables() throws ClientException {
                return null;
            }

            public boolean drop(String tableName) throws ClientException {
                throw new ClientException("mock exception");
            }

            public boolean create(String table) throws ClientException {
                throw new ClientException("mock exception");
            }

            public boolean insert(String tableName,
                    Map<String, Object> record) throws ClientException {
                throw new ClientException("mock exception");
            }

            public TDResultSetBase select(String sql) throws ClientException {
                throw new ClientException("mock exception");
            }

            public TDResultSetBase select(String sql, int queryTimeout) throws ClientException {
                throw new ClientException("mock exception");
            }

            public boolean flush() {
                return false;
            }

            public JobSummary waitJobResult(Job job) throws ClientException {
                return null;
            }

            public Unpacker getJobResult(Job job) throws ClientException {
                return null;
            }
        };
        CommandExecutor exec = new CommandExecutor(clientApi);

        { // insert
            CommandContext context = new CommandContext();
            context.mode = Constants.EXECDIRECT;
            context.sql = "insert into foo (k1, k2) values (1, 'muga')";
            try {
                exec.execute(context);
                fail();
            } catch (Exception e) {
                assertTrue(e instanceof SQLException);
            }
        }
        { // create table
            CommandContext context = new CommandContext();
            context.mode = Constants.EXECDIRECT;
            context.sql = "create table foo(name int)";
            try {
                exec.execute(context);
                fail();
            } catch (Exception e) {
                assertTrue(e instanceof SQLException);
            }
        }
        { // drop table
            CommandContext context = new CommandContext();
            context.mode = Constants.EXECDIRECT;
            context.sql = "drop table foo";
            try {
                exec.execute(context);
                fail();
            } catch (Exception e) {
                assertTrue(e instanceof SQLException);
            }
        }
        { // select
            CommandContext context = new CommandContext();
            context.mode = Constants.EXECDIRECT;
            context.sql = "select v from accesslog";
            try {
                exec.execute(context);
                fail();
            } catch (Exception e) {
                assertTrue(e instanceof SQLException);
            }
        }
    }

    /*
     * throw an exception by invalid context.sql
     */
    @Test
    public void testExecute06() throws Exception {
        ClientAPI clientApi = new NullClientAPI();
        CommandExecutor exec = new CommandExecutor(clientApi);

        CommandContext context = new CommandContext();
        context.mode = Constants.PREPARE;
        context.sql = "illigal statement";
        try {
            exec.execute(context);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof SQLException);
        }
    }

    /*
     * throw an exception by invalid context.sql
     * invalid sql means sql statement without insert/create/drop/select
     */
    @Test
    public void testExecute07() throws Exception {
        ClientAPI clientApi = new NullClientAPI();
        CommandExecutor exec = new CommandExecutor(clientApi);

        CommandContext context = new CommandContext();
        context.mode = Constants.PREPARE;
        context.sql = "update foo set k='muga' where id = 100";
        try {
            exec.execute(context);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof SQLException);
        }
    }

    /*
     * throw an exception by invalid context.sql
     * here sql means a sql statement that includes jdbc parameters like '?'.
     */
    @Test @Ignore
    public void testExecute08() throws Exception { // FIXME #MN
        ClientAPI api = new ClientAPI() {
            public List<DatabaseSummary> showDatabases() throws ClientException {
                return null;
            }

            public List<TableSummary> showTables() throws ClientException {
                return null;
            }

            public boolean drop(String tableName) throws ClientException {
                throw new ClientException("mock exception");
            }

            public boolean create(String table) throws ClientException {
                throw new ClientException("mock exception");
            }

            public boolean insert(String tableName,
                    Map<String, Object> record) throws ClientException {
                throw new ClientException("mock exception");
            }

            public TDResultSetBase select(String sql) throws ClientException {
                throw new ClientException("mock exception");
            }

            public TDResultSetBase select(String sql, int queryTimeout) throws ClientException {
                throw new ClientException("mock exception");
            }

            public boolean flush() {
                return false;
            }

            public JobSummary waitJobResult(Job job) throws ClientException {
                return null;
            }

            public Unpacker getJobResult(Job job) throws ClientException {
                return null;
            }
        };
        CommandExecutor exec = new CommandExecutor(api);

        { // insert
            CommandContext context = new CommandContext();
            context.mode = Constants.PREPARE;
            context.sql = "insert into foo (k1, k2) values (?, 'muga')";
            try {
                exec.execute(context);
                fail();
            } catch (Exception e) {
                assertTrue(e instanceof SQLException);
            }
            context.params = new HashMap<Integer, Object>();
            context.params.put(1, 1);
            try {
                exec.execute(context);
                fail();
            } catch (Exception e) {
                assertTrue(e instanceof SQLException);
            }
        }
    }

    @Test @Ignore
    public void select01() throws Exception {
        CommandExecutor exec = new CommandExecutor(new NullClientAPI());
        CommandContext w = new CommandContext();
        w.mode = Constants.EXECDIRECT;
        w.sql = "select count(*) from mugatbl order by c1 desc";
        exec.execute(w);
    }

    @Test @Ignore
    public void select02() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        TreasureDataClient client = new TreasureDataClient(props);
        TDClientAPI clientApi =
            new TDClientAPI(client, props, new Database("mugadb"));
        CommandExecutor exec = new CommandExecutor(clientApi);

        CommandContext w = new CommandContext();
        w.mode = Constants.EXECDIRECT;
        w.sql = "create table table01(c0 varchar(255), c1 int)";
        exec.execute(w);
    }

    @Test @Ignore
    public void createTable01() throws Exception {
        CommandExecutor exec = new CommandExecutor(new NullClientAPI());
        CommandContext w = new CommandContext();
        w.mode = Constants.EXECDIRECT;
        w.sql = "create table table01(c0 varchar(255), c1 int)";
        exec.execute(w);
    }

    @Test @Ignore
    public void createTable02() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        TreasureDataClient client = new TreasureDataClient(props);
        TDClientAPI clientAdaptor =
            new TDClientAPI(client, props, new Database("mugadb"));
        CommandExecutor exec = new CommandExecutor(clientAdaptor);

        CommandContext w = new CommandContext();
        w.mode = Constants.EXECDIRECT;
        w.sql = "create table table01(c0 varchar(255), c1 int)";
        exec.execute(w);
    }

    @Test @Ignore
    public void dropTable01() throws Exception {
        CommandExecutor exec = new CommandExecutor(new NullClientAPI());
        CommandContext w = new CommandContext();
        w.mode = Constants.EXECDIRECT;
        w.sql = "drop table table02";
        exec.execute(w);
    }

    @Test @Ignore
    public void dropTable02() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        TreasureDataClient client = new TreasureDataClient(props);
        TDClientAPI clientAdaptor =
            new TDClientAPI(client, props, new Database("mugadb"));
        CommandExecutor exec = new CommandExecutor(clientAdaptor);

        CommandContext w = new CommandContext();
        w.mode = Constants.EXECDIRECT;
        w.sql = "drop table table02";
        exec.execute(w);
    }

    @Test @Ignore
    public void insert01() throws Exception {
        CommandExecutor exec = new CommandExecutor(new NullClientAPI());
        CommandContext w = new CommandContext();
        w.mode = Constants.EXECDIRECT;
        w.sql = "insert into table02 (k1, k2, k3) values (2, 'muga', 'nishizawa')";
        exec.execute(w);
    }

    @Test @Ignore
    public void insert02() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        TreasureDataClient client = new TreasureDataClient(props);
        TDClientAPI clientAdaptor =
            new TDClientAPI(client, props, new Database("mugadb"));
        CommandExecutor exec = new CommandExecutor(clientAdaptor);

        String sql = "insert into table01 (%s, %s) values (%s, '%s')";
        for (int i = 0; i < 50; i++) {
            CommandContext w = new CommandContext();
            w.mode = Constants.EXECDIRECT;
            w.sql = String.format(sql, "col1", "col2", "" + i, "muga:" + i);
            exec.execute(w);
        }

        clientAdaptor.flush();
    }

}
