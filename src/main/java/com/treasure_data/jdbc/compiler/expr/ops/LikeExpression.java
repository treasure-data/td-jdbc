package com.treasure_data.jdbc.compiler.expr.ops;

import com.treasure_data.jdbc.compiler.expr.BinaryExpression;
import com.treasure_data.jdbc.compiler.expr.ExpressionVisitor;

public class LikeExpression extends BinaryExpression {
    private boolean not = false;
    private String escape = null;

    public boolean isNot() {
        return not;
    }

    public void setNot(boolean b) {
        not = b;
    }

    public void accept(ExpressionVisitor expressionVisitor) {
        expressionVisitor.visit(this);
    }

    public String getStringExpression() {
        return ((not) ? "NOT " : "") + "LIKE";
    }

    public String toString() {
        String retval = super.toString();
        if (escape != null) {
            retval += " ESCAPE " + "'" + escape + "'";
        }

        return retval;
    }

    public String getEscape() {
        return escape;
    }

    public void setEscape(String escape) {
        this.escape = escape;
    }
}
