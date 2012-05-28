package com.treasure_data.jdbc.compiler.stat;

import java.util.List;

public class Show implements Statement {
    private String type;
    private List<String> parameters;

    public void accept(StatementVisitor statementVisitor) {
        statementVisitor.visit(this);
    }

    public List<String> getParameters() {
        return parameters;
    }

    public String getType() {
        return type;
    }

    public void setParameters(List<String> list) {
        parameters = list;
    }

    public void setType(String string) {
        type = string.toUpperCase();
    }

    public String toString() {
        String sql = "SHOW " + type.toUpperCase();
        if (parameters != null && parameters.size() > 0) {
            for (String id : parameters) {
                sql += " FROM " + id;
            }
        }

        return sql;
    }
}
