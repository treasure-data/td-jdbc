package com.treasure_data.jdbc.compiler.parser;

import java.io.Reader;

import com.treasure_data.jdbc.compiler.SQLParserException;
import com.treasure_data.jdbc.compiler.stat.Statement;

/**
 * Every parser must implements this interface
 */
public interface SQLParser {
    public Statement parse(Reader statementReader) throws SQLParserException;
}
