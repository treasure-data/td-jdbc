package com.treasure_data.jdbc.compiler.stat;

import com.treasure_data.jdbc.compiler.schema.Table;

public interface IntoTableVisitor {
    public void visit(Table tableName);
}
