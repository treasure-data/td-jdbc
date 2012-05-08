package com.treasure_data.jdbc.compiler.stat;

public interface SelectVisitor {
    public void visit(PlainSelect plainSelect);

    public void visit(Union union);
}
