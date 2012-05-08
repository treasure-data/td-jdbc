package com.treasure_data.jdbc.compiler.expr;

/**
 * A "NULL" in a sql statement
 */
public class NullValue implements Expression {
    public void accept(ExpressionVisitor expressionVisitor) {
        expressionVisitor.visit(this);
    }

    public String toString() {
        return "NULL";
    }
}
