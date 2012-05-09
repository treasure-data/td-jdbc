package com.treasure_data.jdbc.internal;

import java.util.Map;

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

    public boolean select(String sql) {
        return false;
    }
}
