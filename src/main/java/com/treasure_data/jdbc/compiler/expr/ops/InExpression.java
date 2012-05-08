package com.treasure_data.jdbc.compiler.expr.ops;

import com.treasure_data.jdbc.compiler.expr.Expression;
import com.treasure_data.jdbc.compiler.expr.ExpressionVisitor;

public class InExpression implements Expression {
    private Expression leftExpression;
    private ItemsList itemsList;
    private boolean not = false;

    public InExpression() {
    }

    public InExpression(Expression leftExpression, ItemsList itemsList) {
        setLeftExpression(leftExpression);
        setItemsList(itemsList);
    }

    public ItemsList getItemsList() {
        return itemsList;
    }

    public Expression getLeftExpression() {
        return leftExpression;
    }

    public void setItemsList(ItemsList list) {
        itemsList = list;
    }

    public void setLeftExpression(Expression expression) {
        leftExpression = expression;
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

    public String toString() {
        return leftExpression + " " + ((not) ? "NOT " : "") + "IN " + itemsList
                + "";
    }
}
