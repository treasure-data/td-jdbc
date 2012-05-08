package com.treasure_data.jdbc.compiler.expr;

/**
 * Every number with a point or a exponential format is a DoubleValue
 */
public class DoubleValue implements Expression {
    private double value;
    private String stringValue;

    public DoubleValue(String value) {
        if (value.charAt(0) == '+') {
            value = value.substring(1);
        }
        this.value = Double.parseDouble(value);
        this.stringValue = value;
    }

    public void accept(ExpressionVisitor expressionVisitor) {
        expressionVisitor.visit(this);
    }

    public double getValue() {
        return value;
    }

    public void setValue(double d) {
        value = d;
    }

    public String toString() {
        return stringValue;
    }
}
