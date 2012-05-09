package com.treasure_data.jdbc.compiler.util.deparser;

import com.treasure_data.jdbc.compiler.expr.Expression;
import com.treasure_data.jdbc.compiler.expr.ExpressionVisitor;
import com.treasure_data.jdbc.compiler.schema.Column;
import com.treasure_data.jdbc.compiler.stat.Update;

/**
 * A class to de-parse (that is, tranform from JSqlParser hierarchy into a
 * string) an {@link com.treasure_data.jdbc.compiler.stat.Update}
 */
public class UpdateDeParser {
    protected StringBuilder buffer;
    protected ExpressionVisitor expressionVisitor;

    public UpdateDeParser() {
    }

    /**
     * @param expressionVisitor
     *            a {@link ExpressionVisitor} to de-parse expressions. It has to
     *            share the same<br>
     *            StringBuilder (buffer parameter) as this object in order to
     *            work
     * @param buffer
     *            the buffer that will be filled with the select
     */
    public UpdateDeParser(ExpressionVisitor expressionVisitor,
            StringBuilder buffer) {
        this.buffer = buffer;
        this.expressionVisitor = expressionVisitor;
    }

    public StringBuilder getBuffer() {
        return buffer;
    }

    public void setBuffer(StringBuilder buffer) {
        this.buffer = buffer;
    }

    public void deParse(Update update) {
        buffer.append("UPDATE " + update.getTable().getWholeTableName()
                + " SET ");
        for (int i = 0; i < update.getColumns().size(); i++) {
            Column column = (Column) update.getColumns().get(i);
            buffer.append(column.getWholeColumnName() + "=");

            Expression expression = (Expression) update.getExpressions().get(i);
            expression.accept(expressionVisitor);
            if (i < update.getColumns().size() - 1) {
                buffer.append(", ");
            }

        }

        if (update.getWhere() != null) {
            buffer.append(" WHERE ");
            update.getWhere().accept(expressionVisitor);
        }

    }

    public ExpressionVisitor getExpressionVisitor() {
        return expressionVisitor;
    }

    public void setExpressionVisitor(ExpressionVisitor visitor) {
        expressionVisitor = visitor;
    }

}
