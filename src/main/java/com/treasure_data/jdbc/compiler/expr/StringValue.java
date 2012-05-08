package com.treasure_data.jdbc.compiler.expr;

/**
 * A string as in 'example_string'
 */
public class StringValue implements Expression {
    private String value = "";

    public StringValue(String escapedValue) {
        // romoving "'" at the start and at the end
        value = escapedValue.substring(1, escapedValue.length() - 1);
    }

    public String getValue() {
        return value;
    }

    public String getNotExcapedValue() {
        StringBuilder buffer = new StringBuilder(value);
        int index = 0;
        int deletesNum = 0;
        while ((index = value.indexOf("''", index)) != -1) {
            buffer.deleteCharAt(index - deletesNum);
            index += 2;
            deletesNum++;
        }
        return buffer.toString();
    }

    public void setValue(String string) {
        value = string;
    }

    public void accept(ExpressionVisitor expressionVisitor) {
        expressionVisitor.visit(this);
    }

    public String toString() {
        return "'" + value + "'";
    }
}
