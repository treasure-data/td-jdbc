package com.treasure_data.jdbc;

import java.sql.SQLWarning;

public class Util {

    public static SQLWarning sqlWarning(String reason,
            String state, Throwable t) {
        return new SQLWarning(reason, state, t);
    }
}
