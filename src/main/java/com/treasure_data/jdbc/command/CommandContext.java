package com.treasure_data.jdbc.command;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.treasure_data.jdbc.TDResultSet;
import com.treasure_data.jdbc.TDResultSetBase;

public class CommandContext {

    public int mode;

    public int queryTimeout = 0; // seconds

    public String sql;

    public List<String> paramList;

    public Map<Integer, Object> params;

    public List<Map<Integer, Object>> params0 = new ArrayList<Map<Integer, Object>>();

    public TDResultSetBase resultSet;

    public CommandContext() {
    }
}
