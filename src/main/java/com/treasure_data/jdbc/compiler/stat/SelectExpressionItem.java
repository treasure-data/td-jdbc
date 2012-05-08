package com.treasure_data.jdbc.compiler.stat;

import com.treasure_data.jdbc.compiler.expr.Expression;

/**
 * An expression as in "SELECT expr1 AS EXPR"
 */
public class SelectExpressionItem implements SelectItem {
    private Expression expression;
    private String alias;

    public String getAlias() {
        return alias;
    }

    public Expression getExpression() {
        return expression;
    }

    public void setAlias(String string) {
        alias = string;
    }

    public void setExpression(Expression expression) {
        this.expression = expression;
    }

    public void accept(SelectItemVisitor selectItemVisitor) {
        selectItemVisitor.visit(this);
    }

    public String toString() {
        return expression + ((alias != null) ? " AS " + alias : "");
    }
}
