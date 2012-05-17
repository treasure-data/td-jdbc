package com.treasure_data.jdbc.command;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public interface ClientAPI {
    boolean createTable(String table);

    boolean insertData(String tableName, Map<String, Object> record);

    boolean flush(); // for debugging

    ResultSet select(String sql) throws SQLException;
}
