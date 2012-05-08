package com.treasure_data.jdbc.compiler.stat;

public interface SelectBody {
    public void accept(SelectVisitor selectVisitor);
}
