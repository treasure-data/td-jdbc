package com.treasure_data.jdbc.compiler.expr.ops;

import java.util.List;

import com.treasure_data.jdbc.compiler.expr.Expression;
import com.treasure_data.jdbc.compiler.stat.PlainSelect;

/**
 * A list of expressions, as in SELECT A FROM TAB WHERE B IN (expr1,expr2,expr3)
 */
public class ExpressionList implements ItemsList {
    private List<Expression> expressions;

    public ExpressionList() {
    }

    public ExpressionList(List<Expression> expressions) {
        this.expressions = expressions;
    }

    public List<Expression> getExpressions() {
        return expressions;
    }

    public void setExpressions(List<Expression> list) {
        expressions = list;
    }

    public void accept(ItemsListVisitor itemsListVisitor) {
        itemsListVisitor.visit(this);
    }

    public String toString() {
        return PlainSelect.getStringList(expressions, true, true);
    }
}
