package com.treasuredata.jdbc.command;

import com.treasuredata.jdbc.TDResultSetBase;

public class CommandContext
{

    public int queryTimeout = 0; // seconds

    public String sql;

    public TDResultSetBase resultSet;

    public CommandContext()
    {
    }
}
