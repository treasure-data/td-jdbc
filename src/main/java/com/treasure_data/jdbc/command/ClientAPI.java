package com.treasure_data.jdbc.command;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.msgpack.unpacker.Unpacker;

import com.treasure_data.client.ClientException;
import com.treasure_data.jdbc.TDResultSetBase;
import com.treasure_data.model.DatabaseSummary;
import com.treasure_data.model.Job;
import com.treasure_data.model.JobSummary;
import com.treasure_data.model.TableSummary;

public interface ClientAPI {
    public static class ExtUnpacker {
        private File file;
        private Unpacker unpacker;

        public ExtUnpacker(File file, Unpacker unpacker) {
            this.file = file;
            this.unpacker = unpacker;
        }

        public File getFile() {
            return file;
        }

        public Unpacker getUnpacker() {
            return unpacker;
        }
    }

    // show database statement
    List<DatabaseSummary> showDatabases() throws ClientException;

    // show table statement
    List<TableSummary> showTables() throws ClientException;

    // drop table statement
    boolean drop(String tableName) throws ClientException;

    // create table statement
    boolean create(String table) throws ClientException;

    // insert statement
    boolean insert(String tableName, Map<String, Object> record) throws ClientException;

    // select statement
    TDResultSetBase select(String sql) throws ClientException;

    TDResultSetBase select(String sql, int queryTimeout) throws ClientException;

    boolean flush(); // for debugging

    JobSummary waitJobResult(Job job) throws ClientException;

    Unpacker getJobResult(Job job) throws ClientException;

    ExtUnpacker getJobResult2(Job job) throws ClientException;

    void close() throws ClientException;
}
