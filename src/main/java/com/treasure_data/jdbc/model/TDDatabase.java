package com.treasure_data.jdbc.model;

public class TDDatabase {
    private String databaseName;

    public TDDatabase(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

}
