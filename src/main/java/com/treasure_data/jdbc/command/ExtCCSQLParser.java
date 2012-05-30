package com.treasure_data.jdbc.command;

import java.io.ByteArrayInputStream;

import com.treasure_data.jdbc.compiler.parser.CCSQLParser;
import com.treasure_data.jdbc.compiler.parser.ParseException;
import com.treasure_data.jdbc.compiler.stat.Select;
import com.treasure_data.jdbc.compiler.stat.Statement;

public class ExtCCSQLParser {

    private String sql;

    private CCSQLParser parser;

    public ExtCCSQLParser(String sql) {
        this.sql = sql;
        this.parser = new CCSQLParser(
                new ByteArrayInputStream(sql.getBytes()));
    }

    public Statement Statement() throws ParseException {
        String s = sql.toUpperCase();
        if (s.startsWith("SELECT ")) {
            return new Select();
        }
        return parser.Statement();
    }
}
