package com.treasure_data.jdbc.compiler.expr.ops;

import com.treasure_data.jdbc.compiler.expr.BinaryExpression;
import com.treasure_data.jdbc.compiler.expr.Expression;
import com.treasure_data.jdbc.compiler.expr.ExpressionVisitor;

public class AndExpression extends BinaryExpression {
    public AndExpression(Expression leftExpression, Expression rightExpression) {
        setLeftExpression(leftExpression);
        setRightExpression(rightExpression);
    }

    public void accept(ExpressionVisitor expressionVisitor) {
        expressionVisitor.visit(this);
    }

    public String getStringExpression() {
        return "AND";
    }
}
