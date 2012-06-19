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
        String s = stripStart(sql.toUpperCase(), " ");

        Statement stat = parseForPentahoReportDesigner(s);
        if (stat != null) {
            return stat;
        }

        if (s.startsWith("SELECT ")) {
            Select select = new Select();
            select.setString(sql);
            return select;
        }
        return parser.Statement();
    }

    private Statement parseForPentahoReportDesigner(String upperCaseSQL) {
        if (!upperCaseSQL.equals("SELECT 1")) {
            return null;
        }

        Select select = new Select();
        select.setString(sql);
        select.selectOne(true);
        return select;
    }

    private static String stripStart(String str, String stripChars) {
        if (str == null) {
            return null;
        }
        int strLen = str.length();
        if (strLen == 0) {
            return str;
        }
        int start = 0;
        if (stripChars == null) {
            while ((start != strLen)
                    && Character.isWhitespace(str.charAt(start))) {
                start++;
            }
        } else if (stripChars.length() == 0) {
            return str;
        } else {
            while ((start != strLen)
                    && (stripChars.indexOf(str.charAt(start)) != -1)) {
                start++;
            }
        }
        return str.substring(start);
    }
}
