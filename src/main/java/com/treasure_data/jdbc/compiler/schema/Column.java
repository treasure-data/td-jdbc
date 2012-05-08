package com.treasure_data.jdbc.compiler.schema;

import com.treasure_data.jdbc.compiler.expr.Expression;
import com.treasure_data.jdbc.compiler.expr.ExpressionVisitor;

/**
 * A column. It can have the table name it belongs to.
 */
public class Column implements Expression {
    private String columnName = "";
    private Table table;

    public Column() {
    }

    public Column(Table table, String columnName) {
        this.table = table;
        this.columnName = columnName;
    }

    public String getColumnName() {
        return columnName;
    }

    public Table getTable() {
        return table;
    }

    public void setColumnName(String string) {
        columnName = string;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    /**
     * @return the name of the column, prefixed with 'tableName' and '.'
     */
    public String getWholeColumnName() {

        String columnWholeName = null;
        String tableWholeName = table.getWholeTableName();

        if (tableWholeName != null && tableWholeName.length() != 0) {
            columnWholeName = tableWholeName + "." + columnName;
        } else {
            columnWholeName = columnName;
        }

        return columnWholeName;

    }

    public void accept(ExpressionVisitor expressionVisitor) {
        expressionVisitor.visit(this);
    }

    public String toString() {
        return getWholeColumnName();
    }
}
