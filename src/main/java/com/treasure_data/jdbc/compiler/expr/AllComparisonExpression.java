package com.treasure_data.jdbc.compiler.expr;

import com.treasure_data.jdbc.compiler.stat.SubSelect;

public class AllComparisonExpression implements Expression {
    private SubSelect subSelect;

    public AllComparisonExpression(SubSelect subSelect) {
        this.subSelect = subSelect;
    }

    public SubSelect GetSubSelect() {
        return subSelect;
    }

    public void accept(ExpressionVisitor expressionVisitor) {
        expressionVisitor.visit(this);
    }

}
