package com.treasure_data.jdbc.compiler.stat;

/**
 * All the columns (as in "SELECT * FROM ...")
 */
public class AllColumns implements SelectItem {
    public void accept(SelectItemVisitor selectItemVisitor) {
        selectItemVisitor.visit(this);
    }

    public String toString() {
        return "*";
    }
}
