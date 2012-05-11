package com.treasure_data.jdbc.command;

import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

public class Wrapper {

    public int mode;

    public String sql;

    public com.treasure_data.jdbc.compiler.stat.Statement compiledSql;

    public List<String> paramList;

    public Map<Integer, Object> params;

    public ResultSet resultSet;

    public Wrapper() {
    }
}
