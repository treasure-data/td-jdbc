package com.treasure_data.jdbc.internal;

import java.util.Map;

public interface ClientAdaptor {
    boolean createTable(String table);

    boolean insertData(String tableName, Map<String, Object> record);

    boolean flush(); // for debugging

    boolean select(String sql);
}
