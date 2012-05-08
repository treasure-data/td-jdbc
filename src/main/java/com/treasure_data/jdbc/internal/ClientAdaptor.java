package com.treasure_data.jdbc.internal;

import java.util.Map;

public interface ClientAdaptor {
    boolean createTable(String table);

    boolean insertData(Map<String, Object> record);

    boolean select(String sql);
}
