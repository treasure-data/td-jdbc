package com.treasure_data.jdbc.compiler.expr;

import com.treasure_data.jdbc.compiler.stat.SubSelect;

public class AnyComparisonExpression implements Expression {
    private SubSelect subSelect;

    public AnyComparisonExpression(SubSelect subSelect) {
        this.subSelect = subSelect;
    }

    public SubSelect GetSubSelect() {
        return subSelect;
    }

    public void accept(ExpressionVisitor expressionVisitor) {
        expressionVisitor.visit(this);
    }
}
