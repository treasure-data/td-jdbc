package com.treasure_data.jdbc.internal;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import com.treasure_data.model.Job;

public interface ClientAdaptor {
    boolean createTable(String table);

    boolean insertData(String tableName, Map<String, Object> record);

    boolean flush(); // for debugging

    ResultSet select(String sql) throws SQLException ;
}
