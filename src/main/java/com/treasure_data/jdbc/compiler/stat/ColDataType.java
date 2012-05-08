package com.treasure_data.jdbc.compiler.stat;

import java.util.List;

public class ColDataType {

    private String dataType;
    private List<String> argumentsStringList;

    public List<String> getArgumentsStringList() {
        return argumentsStringList;
    }

    public String getDataType() {
        return dataType;
    }

    public void setArgumentsStringList(List<String> list) {
        argumentsStringList = list;
    }

    public void setDataType(String string) {
        dataType = string;
    }

    public String toString() {
        return dataType
                + (argumentsStringList != null ? " "
                        + PlainSelect.getStringList(argumentsStringList, true,
                                true) : "");
    }
}
