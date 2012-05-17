package com.treasure_data.jdbc.command;

import java.sql.ResultSet;
import java.util.Map;

import com.treasure_data.client.ClientException;

public interface ClientAPI {
    // create table statement
    boolean create(String table) throws ClientException;

    // insert statement
    boolean insert(String tableName, Map<String, Object> record) throws ClientException;

    // select statement
    ResultSet select(String sql) throws ClientException;

    boolean flush(); // for debugging
}
