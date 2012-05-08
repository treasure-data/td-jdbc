package com.treasure_data.jdbc.compiler.expr;

import java.sql.Time;

/**
 * A Time in the form {t 'hh:mm:ss'}
 */
public class TimeValue implements Expression {
    private Time value;

    public TimeValue(String value) {
        this.value = Time.valueOf(value.substring(1, value.length() - 1));
    }

    public void accept(ExpressionVisitor expressionVisitor) {
        expressionVisitor.visit(this);
    }

    public Time getValue() {
        return value;
    }

    public void setValue(Time d) {
        value = d;
    }

    public String toString() {
        return "{t '" + value + "'}";
    }

}
