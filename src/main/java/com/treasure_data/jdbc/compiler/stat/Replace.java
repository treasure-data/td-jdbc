package com.treasure_data.jdbc.compiler.stat;

import java.util.List;

import com.treasure_data.jdbc.compiler.expr.Expression;
import com.treasure_data.jdbc.compiler.expr.ops.ItemsList;
import com.treasure_data.jdbc.compiler.schema.Column;
import com.treasure_data.jdbc.compiler.schema.Table;

/**
 * The replace statement.
 */
public class Replace implements Statement {
    private Table table;
    private List<Column> columns;
    private ItemsList itemsList;
    private List<Expression> expressions;
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
     * A list of {@link com.treasure_data.jdbc.compiler.schema.Column}s either
     * from a "REPLACE mytab (col1, col2) [...]" or a
     * "REPLACE mytab SET col1=exp1, col2=exp2".
     * 
     * @return a list of {@link com.treasure_data.jdbc.compiler.schema.Column}s
     */
    public List<Column> getColumns() {
        return columns;
    }

    /**
     * An {@link ItemsList} (either from a "REPLACE mytab VALUES (exp1,exp2)" or
     * a "REPLACE mytab SELECT * FROM mytab2") it is null in case of a
     * "REPLACE mytab SET col1=exp1, col2=exp2"
     */
    public ItemsList getItemsList() {
        return itemsList;
    }

    public void setColumns(List<Column> list) {
        columns = list;
    }

    public void setItemsList(ItemsList list) {
        itemsList = list;
    }

    /**
     * A list of {@link com.treasure_data.jdbc.compiler.expr.Expression}s (from
     * a "REPLACE mytab SET col1=exp1, col2=exp2"). <br>
     * it is null in case of a "REPLACE mytab (col1, col2) [...]"
     */
    public List<Expression> getExpressions() {
        return expressions;
    }

    public void setExpressions(List<Expression> list) {
        expressions = list;
    }

    public boolean isUseValues() {
        return useValues;
    }

    public void setUseValues(boolean useValues) {
        this.useValues = useValues;
    }

    public String toString() {
        String sql = "REPLACE " + table;

        if (expressions != null && columns != null) {
            // the SET col1=exp1, col2=exp2 case
            sql += " SET ";
            // each element from expressions match up with a column from
            // columns.
            for (int i = 0, s = columns.size(); i < s; i++) {
                sql += "" + columns.get(i) + "=" + expressions.get(i);
                sql += (i < s - 1) ? ", " : "";
            }
        } else if (columns != null) {
            // the REPLACE mytab (col1, col2) [...] case
            sql += " " + PlainSelect.getStringList(columns, true, true);
        }

        if (itemsList != null) {
            // REPLACE mytab SELECT * FROM mytab2
            // or VALUES ('as', ?, 565)

            if (useValues) {
                sql += " VALUES";
            }

            sql += " " + itemsList;
        }

        return sql;
    }

}
