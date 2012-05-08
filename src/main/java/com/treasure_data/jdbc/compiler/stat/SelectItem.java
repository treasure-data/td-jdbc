package com.treasure_data.jdbc.compiler.stat;

/**
 * Anything between "SELECT" and "FROM"<BR>
 * (that is, any column or expression etc to be retrieved with the query)
 */
public interface SelectItem {
    public void accept(SelectItemVisitor selectItemVisitor);
}
