package com.treasure_data.jdbc.command;

import com.treasure_data.jdbc.TDResultSetBase;

public class CommandContext
{

    public int queryTimeout = 0; // seconds

    public String sql;

    public TDResultSetBase resultSet;

    public CommandContext()
    {
    }
}
