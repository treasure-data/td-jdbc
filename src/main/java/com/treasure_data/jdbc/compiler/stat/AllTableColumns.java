package com.treasure_data.jdbc.compiler.stat;

/**
 * All the columns of a table (as in "SELECT TableName.* FROM ...")
 */
import com.treasure_data.jdbc.compiler.schema.Table;

public class AllTableColumns implements SelectItem {
    private Table table;

    public AllTableColumns() {
    }

    public AllTableColumns(Table tableName) {
        this.table = tableName;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public void accept(SelectItemVisitor selectItemVisitor) {
        selectItemVisitor.visit(this);
    }

    public String toString() {
        return table + ".*";
    }

}
