package com.treasure_data.jdbc.compiler.stat;

import com.treasure_data.jdbc.compiler.schema.Table;

public interface FromItemVisitor {
    public void visit(Table tableName);

    public void visit(SubSelect subSelect);

    public void visit(SubJoin subjoin);
}
