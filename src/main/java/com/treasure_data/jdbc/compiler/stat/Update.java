package com.treasure_data.jdbc.compiler.stat;

import java.util.List;

import com.treasure_data.jdbc.compiler.expr.Expression;
import com.treasure_data.jdbc.compiler.schema.Column;
import com.treasure_data.jdbc.compiler.schema.Table;

/**
 * The update statement.
 */
public class Update implements Statement {
    private Table table;
    private Expression where;
    private List<Column> columns;
    private List<Expression> expressions;

    public void accept(StatementVisitor statementVisitor) {
        statementVisitor.visit(this);
    }

    public Table getTable() {
        return table;
    }

    public Expression getWhere() {
        return where;
    }

    public void setTable(Table name) {
        table = name;
    }

    public void setWhere(Expression expression) {
        where = expression;
    }

    /**
     * The {@link com.treasure_data.jdbc.compiler.schema.Column}s in this update
     * (as col1 and col2 in UPDATE col1='a', col2='b')
     * 
     * @return a list of {@link com.treasure_data.jdbc.compiler.schema.Column}s
     */
    public List<Column> getColumns() {
        return columns;
    }

    /**
     * The {@link Expression}s in this update (as 'a' and 'b' in UPDATE
     * col1='a', col2='b')
     * 
     * @return a list of {@link Expression}s
     */
    public List<Expression> getExpressions() {
        return expressions;
    }

    public void setColumns(List<Column> list) {
        columns = list;
    }

    public void setExpressions(List<Expression> list) {
        expressions = list;
    }

}
