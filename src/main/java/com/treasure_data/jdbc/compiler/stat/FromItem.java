package com.treasure_data.jdbc.compiler.stat;

/**
 * An item in a "SELECT [...] FROM item1" statement. (for example a table or a
 * sub-select)
 */
public interface FromItem {
    public void accept(FromItemVisitor fromItemVisitor);

    public String getAlias();

    public void setAlias(String alias);

}
