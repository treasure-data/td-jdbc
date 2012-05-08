package com.treasure_data.jdbc.compiler.expr;

public interface Expression {
    public void accept(ExpressionVisitor expressionVisitor);
}
