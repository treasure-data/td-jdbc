package com.treasure_data.jdbc.compiler.stat;

import java.util.List;

import com.treasure_data.jdbc.compiler.expr.ops.ItemsList;
import com.treasure_data.jdbc.compiler.schema.Column;
import com.treasure_data.jdbc.compiler.schema.Table;

/**
 * The insert statement. Every column name in <code>columnNames</code> matches
 * an item in <code>itemsList</code>
 */
public class Insert implements Statement {
    private Table table;
    private List<Column> columns;
    private ItemsList itemsList;
    private boolean useValues = true;

    public void accept(StatementVisitor statementVisitor) {
        statementVisitor.visit(this);
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table name) {
        table = name;
    }

    /**
     * Get the columns (found in "INSERT INTO (col1,col2..) [...]" )
     * 
     * @return a list of {@link com.treasure_data.jdbc.compiler.schema.Column}
     */
    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> list) {
        columns = list;
    }

    /**
     * Get the values (as VALUES (...) or SELECT)
     * 
     * @return the values of the insert
     */
    public ItemsList getItemsList() {
        return itemsList;
    }

    public void setItemsList(ItemsList list) {
        itemsList = list;
    }

    public boolean isUseValues() {
        return useValues;
    }

    public void setUseValues(boolean useValues) {
        this.useValues = useValues;
    }

    public String toString() {
        String sql = "";

        sql = "INSERT INTO ";
        sql += table + " ";
        sql += ((columns != null) ? PlainSelect.getStringList(columns, true,
                true) + " " : "");

        if (useValues) {
            sql += "VALUES " + itemsList + "";
        } else {
            sql += "" + itemsList + "";
        }

        return sql;
    }

}
