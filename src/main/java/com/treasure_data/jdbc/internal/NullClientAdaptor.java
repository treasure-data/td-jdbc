package com.treasure_data.jdbc.internal;

import java.util.Map;

public class NullClientAdaptor implements ClientAdaptor {

    @Override
    public boolean createTable(String table) {
        return true;
    }

    @Override
    public boolean insertData(Map<String, Object> record) {
        return true;
    }

    @Override
    public boolean select(String sql) {
        return false;
    }
}
