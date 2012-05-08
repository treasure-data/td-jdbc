package com.treasure_data.jdbc.compiler.expr.ops;

/**
 * Values of an "INSERT" statement (for example a SELECT or a list of
 * expressions)
 */
public interface ItemsList {
    public void accept(ItemsListVisitor itemsListVisitor);
}
