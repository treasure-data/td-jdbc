package com.treasure_data.jdbc.compiler.stat;

public interface SelectItemVisitor {
    public void visit(AllColumns allColumns);

    public void visit(AllTableColumns allTableColumns);

    public void visit(SelectExpressionItem selectExpressionItem);

}
