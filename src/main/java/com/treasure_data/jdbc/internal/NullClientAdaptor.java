package com.treasure_data.jdbc.internal;

import java.util.Map;

import com.treasure_data.model.Job;

public class NullClientAdaptor implements ClientAdaptor {

    public boolean createTable(String table) {
        return true;
    }

    public boolean insertData(String tableName, Map<String, Object> record) {
        return true;
    }

    public boolean flush() {
        return true;
    }

    public Job select(String sql) {
        return null;
    }
}
