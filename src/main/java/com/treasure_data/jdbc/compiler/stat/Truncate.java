package com.treasure_data.jdbc.compiler.stat;

import com.treasure_data.jdbc.compiler.schema.Table;

/**
 * A TRUNCATE TABLE statement
 */
public class Truncate implements Statement {
    private Table table;

    public void accept(StatementVisitor statementVisitor) {
        statementVisitor.visit(this);
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public String toString() {
        return "TRUNCATE TABLE " + table;
    }
}
