package com.treasure_data.jdbc.command;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.treasure_data.jdbc.TDResultSet;
import com.treasure_data.jdbc.TDResultSetBase;
import com.treasure_data.jdbc.compiler.stat.Statement;

public class CommandContext {

    public int mode;

    public String sql;

    public Statement compiledSql;

    public List<String> paramList;

    public Map<Integer, Object> params;

    public List<Map<Integer, Object>> params0 = new ArrayList<Map<Integer, Object>>();

    public TDResultSetBase resultSet;

    public CommandContext() {
    }
}
