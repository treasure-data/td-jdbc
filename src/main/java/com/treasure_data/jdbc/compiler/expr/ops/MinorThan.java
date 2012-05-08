package com.treasure_data.jdbc.compiler.expr.ops;

import com.treasure_data.jdbc.compiler.expr.BinaryExpression;
import com.treasure_data.jdbc.compiler.expr.ExpressionVisitor;

public class MinorThan extends BinaryExpression {
    public void accept(ExpressionVisitor expressionVisitor) {
        expressionVisitor.visit(this);
    }

    public String getStringExpression() {
        return "<";
    }
}
