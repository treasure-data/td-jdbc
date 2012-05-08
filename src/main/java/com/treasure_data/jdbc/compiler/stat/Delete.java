package com.treasure_data.jdbc.compiler.stat;

import com.treasure_data.jdbc.compiler.expr.Expression;
import com.treasure_data.jdbc.compiler.schema.Table;

public class Delete implements Statement {
    private Table table;
    private Expression where;

    public void accept(StatementVisitor statementVisitor) {
        statementVisitor.visit(this);
    }

    public Table getTable() {
        return table;
    }

    public Expression getWhere() {
        return where;
    }

    public void setTable(Table name) {
        table = name;
    }

    public void setWhere(Expression expression) {
        where = expression;
    }

    public String toString() {
        return "DELETE FROM " + table
                + ((where != null) ? " WHERE " + where : "");
    }
}
