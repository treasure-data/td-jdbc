package com.treasure_data.jdbc.compiler.util.deparser;

import java.util.Iterator;

import com.treasure_data.jdbc.compiler.expr.Expression;
import com.treasure_data.jdbc.compiler.expr.ExpressionVisitor;
import com.treasure_data.jdbc.compiler.expr.ops.ExpressionList;
import com.treasure_data.jdbc.compiler.expr.ops.ItemsListVisitor;
import com.treasure_data.jdbc.compiler.schema.Column;
import com.treasure_data.jdbc.compiler.stat.Insert;
import com.treasure_data.jdbc.compiler.stat.SelectVisitor;
import com.treasure_data.jdbc.compiler.stat.SubSelect;

/**
 * A class to de-parse (that is, tranform from JSqlParser hierarchy into a
 * string) an {@link com.treasure_data.jdbc.compiler.stat.Insert}
 */
public class InsertDeParser implements ItemsListVisitor {
    protected StringBuilder buffer;
    protected ExpressionVisitor expressionVisitor;
    protected SelectVisitor selectVisitor;

    public InsertDeParser() {
    }

    /**
     * @param expressionVisitor
     *            a {@link ExpressionVisitor} to de-parse
     *            {@link com.treasure_data.jdbc.compiler.expr.Expression}s. It
     *            has to share the same<br>
     *            StringBuilder (buffer parameter) as this object in order to
     *            work
     * @param selectVisitor
     *            a {@link SelectVisitor} to de-parse
     *            {@link com.treasure_data.jdbc.compiler.stat.Select}s. It has
     *            to share the same<br>
     *            StringBuilder (buffer parameter) as this object in order to
     *            work
     * @param buffer
     *            the buffer that will be filled with the insert
     */
    public InsertDeParser(ExpressionVisitor expressionVisitor,
            SelectVisitor selectVisitor, StringBuilder buffer) {
        this.buffer = buffer;
        this.expressionVisitor = expressionVisitor;
        this.selectVisitor = selectVisitor;
    }

    public StringBuilder getBuffer() {
        return buffer;
    }

    public void setBuffer(StringBuilder buffer) {
        this.buffer = buffer;
    }

    public void deParse(Insert insert) {
        buffer.append("INSERT INTO ");
        buffer.append(insert.getTable().getWholeTableName());
        if (insert.getColumns() != null) {
            buffer.append("(");
            for (Iterator<Column> iter = insert.getColumns().iterator(); iter
                    .hasNext();) {
                Column column = iter.next();
                buffer.append(column.getColumnName());
                if (iter.hasNext()) {
                    buffer.append(", ");
                }
            }
            buffer.append(")");
        }

        insert.getItemsList().accept(this);

    }

    public void visit(ExpressionList expressionList) {
        buffer.append(" VALUES (");
        for (Iterator<Expression> iter = expressionList.getExpressions()
                .iterator(); iter.hasNext();) {
            Expression expression = iter.next();
            expression.accept(expressionVisitor);
            if (iter.hasNext())
                buffer.append(", ");
        }
        buffer.append(")");
    }

    public void visit(SubSelect subSelect) {
        subSelect.getSelectBody().accept(selectVisitor);
    }

    public ExpressionVisitor getExpressionVisitor() {
        return expressionVisitor;
    }

    public SelectVisitor getSelectVisitor() {
        return selectVisitor;
    }

    public void setExpressionVisitor(ExpressionVisitor visitor) {
        expressionVisitor = visitor;
    }

    public void setSelectVisitor(SelectVisitor visitor) {
        selectVisitor = visitor;
    }

}
