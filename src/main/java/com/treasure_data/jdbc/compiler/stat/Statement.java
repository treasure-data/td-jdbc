package com.treasure_data.jdbc.compiler.stat;

/**
 * An operation on the db (SELECT, UPDATE ecc.)
 */
public interface Statement {
    public void accept(StatementVisitor statementVisitor);
}
