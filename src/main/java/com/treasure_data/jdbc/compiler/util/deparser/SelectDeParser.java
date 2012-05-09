package com.treasure_data.jdbc.compiler.util.deparser;

import java.util.Iterator;
import java.util.List;

import com.treasure_data.jdbc.compiler.expr.Expression;
import com.treasure_data.jdbc.compiler.expr.ExpressionVisitor;
import com.treasure_data.jdbc.compiler.schema.Column;
import com.treasure_data.jdbc.compiler.schema.Table;
import com.treasure_data.jdbc.compiler.stat.AllColumns;
import com.treasure_data.jdbc.compiler.stat.AllTableColumns;
import com.treasure_data.jdbc.compiler.stat.FromItem;
import com.treasure_data.jdbc.compiler.stat.FromItemVisitor;
import com.treasure_data.jdbc.compiler.stat.Join;
import com.treasure_data.jdbc.compiler.stat.Limit;
import com.treasure_data.jdbc.compiler.stat.OrderByElement;
import com.treasure_data.jdbc.compiler.stat.OrderByVisitor;
import com.treasure_data.jdbc.compiler.stat.PlainSelect;
import com.treasure_data.jdbc.compiler.stat.SelectExpressionItem;
import com.treasure_data.jdbc.compiler.stat.SelectItem;
import com.treasure_data.jdbc.compiler.stat.SelectItemVisitor;
import com.treasure_data.jdbc.compiler.stat.SelectVisitor;
import com.treasure_data.jdbc.compiler.stat.SubJoin;
import com.treasure_data.jdbc.compiler.stat.SubSelect;
import com.treasure_data.jdbc.compiler.stat.Top;
import com.treasure_data.jdbc.compiler.stat.Union;

/**
 * A class to de-parse (that is, tranform from JSqlParser hierarchy into a
 * string) a {@link com.treasure_data.jdbc.compiler.stat.Select}
 */
public class SelectDeParser implements SelectVisitor, OrderByVisitor,
        SelectItemVisitor, FromItemVisitor {
    protected StringBuilder buffer;
    protected ExpressionVisitor expressionVisitor;

    public SelectDeParser() {
    }

    /**
     * @param expressionVisitor
     *            a {@link ExpressionVisitor} to de-parse expressions. It has to
     *            share the same<br>
     *            StringBuilder (buffer parameter) as this object in order to
     *            work
     * @param buffer
     *            the buffer that will be filled with the select
     */
    public SelectDeParser(ExpressionVisitor expressionVisitor,
            StringBuilder buffer) {
        this.buffer = buffer;
        this.expressionVisitor = expressionVisitor;
    }

    public void visit(PlainSelect plainSelect) {
        buffer.append("SELECT ");
        Top top = plainSelect.getTop();
        if (top != null)
            buffer.append(top).append(" ");
        if (plainSelect.getDistinct() != null) {
            buffer.append("DISTINCT ");
            if (plainSelect.getDistinct().getOnSelectItems() != null) {
                buffer.append("ON (");
                for (Iterator<SelectItem> iter = plainSelect.getDistinct()
                        .getOnSelectItems().iterator(); iter.hasNext();) {
                    SelectItem selectItem = iter.next();
                    selectItem.accept(this);
                    if (iter.hasNext()) {
                        buffer.append(", ");
                    }
                }
                buffer.append(") ");
            }

        }

        for (Iterator<SelectItem> iter = plainSelect.getSelectItems()
                .iterator(); iter.hasNext();) {
            SelectItem selectItem = iter.next();
            selectItem.accept(this);
            if (iter.hasNext()) {
                buffer.append(", ");
            }
        }

        buffer.append(" ");

        if (plainSelect.getFromItem() != null) {
            buffer.append("FROM ");
            plainSelect.getFromItem().accept(this);
        }

        if (plainSelect.getJoins() != null) {
            for (Iterator<Join> iter = plainSelect.getJoins().iterator(); iter
                    .hasNext();) {
                Join join = iter.next();
                deparseJoin(join);
            }
        }

        if (plainSelect.getWhere() != null) {
            buffer.append(" WHERE ");
            plainSelect.getWhere().accept(expressionVisitor);
        }

        if (plainSelect.getGroupByColumnReferences() != null) {
            buffer.append(" GROUP BY ");
            for (Iterator<Expression> iter = plainSelect
                    .getGroupByColumnReferences().iterator(); iter.hasNext();) {
                Expression columnReference = iter.next();
                columnReference.accept(expressionVisitor);
                if (iter.hasNext()) {
                    buffer.append(", ");
                }
            }
        }

        if (plainSelect.getHaving() != null) {
            buffer.append(" HAVING ");
            plainSelect.getHaving().accept(expressionVisitor);
        }

        if (plainSelect.getOrderByElements() != null) {
            deparseOrderBy(plainSelect.getOrderByElements());
        }

        if (plainSelect.getLimit() != null) {
            deparseLimit(plainSelect.getLimit());
        }

    }

    public void visit(Union union) {
        for (Iterator<PlainSelect> iter = union.getPlainSelects().iterator(); iter
                .hasNext();) {
            buffer.append("(");
            PlainSelect plainSelect = iter.next();
            plainSelect.accept(this);
            buffer.append(")");
            if (iter.hasNext()) {
                buffer.append(" UNION ");
                if (union.isAll()) {
                    buffer.append("ALL ");// should UNION be a BinaryExpression
                                          // ?
                }
            }

        }

        if (union.getOrderByElements() != null) {
            deparseOrderBy(union.getOrderByElements());
        }

        if (union.getLimit() != null) {
            deparseLimit(union.getLimit());
        }

    }

    public void visit(OrderByElement orderBy) {
        orderBy.getExpression().accept(expressionVisitor);
        if (!orderBy.isAsc())
            buffer.append(" DESC");
    }

    public void visit(Column column) {
        buffer.append(column.getWholeColumnName());
    }

    public void visit(AllColumns allColumns) {
        buffer.append("*");
    }

    public void visit(AllTableColumns allTableColumns) {
        buffer.append(allTableColumns.getTable().getWholeTableName() + ".*");
    }

    public void visit(SelectExpressionItem selectExpressionItem) {
        selectExpressionItem.getExpression().accept(expressionVisitor);
        if (selectExpressionItem.getAlias() != null) {
            buffer.append(" AS " + selectExpressionItem.getAlias());
        }

    }

    public void visit(SubSelect subSelect) {
        buffer.append("(");
        subSelect.getSelectBody().accept(this);
        buffer.append(")");
        String alias = subSelect.getAlias();
        if (alias != null) {
            buffer.append(" AS ").append(alias);
        }
    }

    public void visit(Table tableName) {
        buffer.append(tableName.getWholeTableName());
        String alias = tableName.getAlias();
        if (alias != null && !alias.isEmpty()) {
            buffer.append(" AS " + alias);
        }
    }

    public void deparseOrderBy(List<OrderByElement> orderByElements) {
        buffer.append(" ORDER BY ");
        for (Iterator<OrderByElement> iter = orderByElements.iterator(); iter
                .hasNext();) {
            OrderByElement orderByElement = iter.next();
            orderByElement.accept(this);
            if (iter.hasNext()) {
                buffer.append(", ");
            }
        }
    }

    public void deparseLimit(Limit limit) {
        // LIMIT n OFFSET skip
        if (limit.isRowCountJdbcParameter()) {
            buffer.append(" LIMIT ");
            buffer.append("?");
        } else if (limit.getRowCount() != 0) {
            buffer.append(" LIMIT ");
            buffer.append(limit.getRowCount());
        }

        if (limit.isOffsetJdbcParameter()) {
            buffer.append(" OFFSET ?");
        } else if (limit.getOffset() != 0) {
            buffer.append(" OFFSET " + limit.getOffset());
        }

    }

    public StringBuilder getBuffer() {
        return buffer;
    }

    public void setBuffer(StringBuilder buffer) {
        this.buffer = buffer;
    }

    public ExpressionVisitor getExpressionVisitor() {
        return expressionVisitor;
    }

    public void setExpressionVisitor(ExpressionVisitor visitor) {
        expressionVisitor = visitor;
    }

    public void visit(SubJoin subjoin) {
        buffer.append("(");
        subjoin.getLeft().accept(this);
        deparseJoin(subjoin.getJoin());
        buffer.append(")");
    }

    public void deparseJoin(Join join) {
        if (join.isSimple())
            buffer.append(", ");
        else {

            if (join.isRight())
                buffer.append(" RIGHT");
            else if (join.isNatural())
                buffer.append(" NATURAL");
            else if (join.isFull())
                buffer.append(" FULL");
            else if (join.isLeft())
                buffer.append(" LEFT");

            if (join.isOuter())
                buffer.append(" OUTER");
            else if (join.isInner())
                buffer.append(" INNER");

            buffer.append(" JOIN ");

        }

        FromItem fromItem = join.getRightItem();
        fromItem.accept(this);
        if (join.getOnExpression() != null) {
            buffer.append(" ON ");
            join.getOnExpression().accept(expressionVisitor);
        }
        if (join.getUsingColumns() != null) {
            buffer.append(" USING (");
            for (Iterator<Column> iterator = join.getUsingColumns().iterator(); iterator
                    .hasNext();) {
                Column column = iterator.next();
                buffer.append(column.getWholeColumnName());
                if (iterator.hasNext()) {
                    buffer.append(", ");
                }
            }
            buffer.append(")");
        }

    }

}
