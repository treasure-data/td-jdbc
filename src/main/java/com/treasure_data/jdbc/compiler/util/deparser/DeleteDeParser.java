package com.treasure_data.jdbc.compiler.util.deparser;

import com.treasure_data.jdbc.compiler.expr.ExpressionVisitor;
import com.treasure_data.jdbc.compiler.stat.Delete;

/**
 * A class to de-parse (that is, tranform from JSqlParser hierarchy into a
 * string) a {@link com.treasure_data.jdbc.compiler.stat.Delete}
 */
public class DeleteDeParser {
    protected StringBuilder buffer;
    protected ExpressionVisitor expressionVisitor;

    public DeleteDeParser() {
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
    public DeleteDeParser(ExpressionVisitor expressionVisitor,
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

    public void deParse(Delete delete) {
        buffer.append("DELETE FROM " + delete.getTable().getWholeTableName());
        if (delete.getWhere() != null) {
            buffer.append(" WHERE ");
            delete.getWhere().accept(expressionVisitor);
        }

    }

    public ExpressionVisitor getExpressionVisitor() {
        return expressionVisitor;
    }

    public void setExpressionVisitor(ExpressionVisitor visitor) {
        expressionVisitor = visitor;
    }

}
