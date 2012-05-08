package com.treasure_data.jdbc.compiler.expr.ops;

import com.treasure_data.jdbc.compiler.expr.BinaryExpression;
import com.treasure_data.jdbc.compiler.expr.Expression;
import com.treasure_data.jdbc.compiler.expr.ExpressionVisitor;

public class OrExpression extends BinaryExpression {
    public OrExpression(Expression leftExpression, Expression rightExpression) {
        setLeftExpression(leftExpression);
        setRightExpression(rightExpression);
    }

    public void accept(ExpressionVisitor expressionVisitor) {
        expressionVisitor.visit(this);
    }

    public String getStringExpression() {
        return "OR";
    }
}
