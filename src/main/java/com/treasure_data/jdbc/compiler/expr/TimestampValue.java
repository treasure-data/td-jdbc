package com.treasure_data.jdbc.compiler.expr;

import java.sql.Timestamp;

/**
 * A Timestamp in the form {ts 'yyyy-mm-dd hh:mm:ss.f . . .'}
 */
public class TimestampValue implements Expression {
    private Timestamp value;

    public TimestampValue(String value) {
        this.value = Timestamp.valueOf(value.substring(1, value.length() - 1));
    }

    public void accept(ExpressionVisitor expressionVisitor) {
        expressionVisitor.visit(this);
    }

    public Timestamp getValue() {
        return value;
    }

    public void setValue(Timestamp d) {
        value = d;
    }

    public String toString() {
        return "{ts '" + value + "'}";
    }
}
