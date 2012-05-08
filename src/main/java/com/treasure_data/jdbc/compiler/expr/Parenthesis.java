package com.treasure_data.jdbc.compiler.expr;

/**
 * It represents an expression like "(" expression ")"
 */
public class Parenthesis implements Expression {
    private Expression expression;
    private boolean not = false;

    public Parenthesis() {
    }

    public Parenthesis(Expression expression) {
        setExpression(expression);
    }

    public Expression getExpression() {
        return expression;
    }

    public void setExpression(Expression expression) {
        this.expression = expression;
    }

    public void accept(ExpressionVisitor expressionVisitor) {
        expressionVisitor.visit(this);
    }

    public void setNot() {
        not = true;
    }

    public boolean isNot() {
        return not;
    }

    public String toString() {
        return (not ? "NOT " : "") + "(" + expression + ")";
    }
}
