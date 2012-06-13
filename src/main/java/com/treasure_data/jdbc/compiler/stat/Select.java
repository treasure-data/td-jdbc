package com.treasure_data.jdbc.compiler.stat;

import java.util.Iterator;
import java.util.List;

public class Select implements Statement {
    private SelectBody selectBody;
    private List<WithItem> withItemsList;
    private boolean selectOne = false;

    public void accept(StatementVisitor statementVisitor) {
        statementVisitor.visit(this);
    }

    public SelectBody getSelectBody() {
        return selectBody;
    }

    public void setSelectBody(SelectBody body) {
        selectBody = body;
    }

    public void selectOne(boolean flag) {
        selectOne = flag;
    }

    public boolean isSelectOne() {
        return selectOne;
    }

    public String toString() {
        StringBuilder retval = new StringBuilder();
        if (withItemsList != null && !withItemsList.isEmpty()) {
            retval.append("WITH ");
            for (Iterator<WithItem> iter = withItemsList.iterator(); iter
                    .hasNext();) {
                WithItem withItem = (WithItem) iter.next();
                retval.append(withItem);
                if (iter.hasNext())
                    retval.append(",");
                retval.append(" ");
            }
        }
        retval.append(selectBody);
        return retval.toString();
    }

    public List<WithItem> getWithItemsList() {
        return withItemsList;
    }

    public void setWithItemsList(List<WithItem> withItemsList) {
        this.withItemsList = withItemsList;
    }
}
