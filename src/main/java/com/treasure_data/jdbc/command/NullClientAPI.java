package com.treasure_data.jdbc.command;

import java.sql.ResultSet;
import java.util.Map;

import com.treasure_data.client.ClientException;

public class NullClientAPI implements ClientAPI {
    public NullClientAPI() {
    }

    public boolean drop(String tableName) throws ClientException {
        return true;
    }

    public boolean create(String table) throws ClientException {
        return true;
    }

    public boolean insert(String tableName, Map<String, Object> record) throws ClientException {
        return true;
    }

    public boolean flush() {
        return true;
    }

    public ResultSet select(String sql) throws ClientException {
        return null;
    }

}
