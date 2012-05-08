package com.treasure_data.jdbc.compiler.expr.ops;

import com.treasure_data.jdbc.compiler.stat.SubSelect;

public interface ItemsListVisitor {
    public void visit(SubSelect subSelect);

    public void visit(ExpressionList expressionList);
}
