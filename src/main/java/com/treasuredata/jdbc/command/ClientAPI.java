package com.treasuredata.jdbc.command;

import com.treasure_data.client.ClientException;
import com.treasuredata.jdbc.TDResultSetBase;
import com.treasuredata.jdbc.TDResultSetMetaData;
import com.treasure_data.model.DatabaseSummary;
import com.treasure_data.model.Job;
import com.treasure_data.model.JobSummary;
import com.treasure_data.model.TableSummary;
import org.msgpack.unpacker.Unpacker;

import java.io.File;
import java.util.List;

public interface ClientAPI
{
    public static class ExtUnpacker
    {
        private File file;
        private Unpacker unpacker;

        public ExtUnpacker(File file, Unpacker unpacker)
        {
            this.file = file;
            this.unpacker = unpacker;
        }

        public File getFile()
        {
            return file;
        }

        public Unpacker getUnpacker()
        {
            return unpacker;
        }
    }

    // show all databases statement
    List<DatabaseSummary> showDatabases()
            throws ClientException;

    // show current database statement
    DatabaseSummary showDatabase()
            throws ClientException;

    // show table statement
    List<TableSummary> showTables()
            throws ClientException;

    // select statement
    TDResultSetBase select(String sql)
            throws ClientException;

    TDResultSetBase select(String sql, int queryTimeout)
            throws ClientException;

    TDResultSetMetaData getMetaDataWithSelect1();

    boolean flush(); // for debugging

    JobSummary waitJobResult(Job job)
            throws ClientException;

    Unpacker getJobResult(Job job)
            throws ClientException;

    ExtUnpacker getJobResult2(Job job)
            throws ClientException;

    void close()
            throws ClientException;
}
