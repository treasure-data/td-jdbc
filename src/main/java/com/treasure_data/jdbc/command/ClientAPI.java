package com.treasure_data.jdbc.command;

import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

import org.msgpack.unpacker.Unpacker;

import com.treasure_data.client.ClientException;
import com.treasure_data.model.Job;
import com.treasure_data.model.JobSummary;
import com.treasure_data.model.TableSummary;

public interface ClientAPI {
    // show table statement
    List<TableSummary> showTables() throws ClientException;

    // drop table statement
    boolean drop(String tableName) throws ClientException;

    // create table statement
    boolean create(String table) throws ClientException;

    // insert statement
    boolean insert(String tableName, Map<String, Object> record) throws ClientException;

    // select statement
    ResultSet select(String sql) throws ClientException;

    boolean flush(); // for debugging

    JobSummary waitJobResult(Job job) throws ClientException;

    Unpacker getJobResult(Job job) throws ClientException;
}
