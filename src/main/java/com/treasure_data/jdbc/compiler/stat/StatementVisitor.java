package com.treasure_data.jdbc.compiler.stat;

public interface StatementVisitor {
    public void visit(Select select);

    public void visit(Delete delete);

    public void visit(Update update);

    public void visit(Insert insert);

    public void visit(Replace replace);

    public void visit(Drop drop);

    public void visit(Truncate truncate);

    public void visit(CreateTable createTable);

}
