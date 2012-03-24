package com.treasure_data.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public abstract class TreasureDataMetaDataResultSet<M> extends
        TreasureDataBaseResultSet {
    protected final List<M> data;

    public TreasureDataMetaDataResultSet(final List<String> columnNames,
            final List<String> columnTypes, final List<M> data)
            throws SQLException {
        if (data != null) {
            this.data = new ArrayList<M>(data);
        } else {
            this.data = new ArrayList<M>();
        }
        if (columnNames != null) {
            this.columnNames = new ArrayList<String>(columnNames);
        } else {
            this.columnNames = new ArrayList<String>();
        }
        if (columnTypes != null) {
            this.columnTypes = new ArrayList<String>(columnTypes);
        } else {
            this.columnTypes = new ArrayList<String>();
        }
    }

    @Override
    public void close() throws SQLException {
    }

}
