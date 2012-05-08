package com.treasure_data.jdbc.compiler.expr.ops;

import com.treasure_data.jdbc.compiler.expr.Expression;
import com.treasure_data.jdbc.compiler.expr.ExpressionVisitor;

public class ExistsExpression implements Expression {
    private Expression rightExpression;
    private boolean not = false;

    public Expression getRightExpression() {
        return rightExpression;
    }

    public void setRightExpression(Expression expression) {
        rightExpression = expression;
    }

    public boolean isNot() {
        return not;
    }

    public void setNot(boolean b) {
        not = b;
    }

    public void accept(ExpressionVisitor expressionVisitor) {
        expressionVisitor.visit(this);
    }

    public String getStringExpression() {
        return ((not) ? "NOT " : "") + "EXISTS";
    }

    public String toString() {
        return getStringExpression() + " " + rightExpression.toString();
    }
}
