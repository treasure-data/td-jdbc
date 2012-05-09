package com.treasure_data.jdbc.compiler.util.deparser;

import java.util.Iterator;
import java.util.List;

import com.treasure_data.jdbc.compiler.expr.AllComparisonExpression;
import com.treasure_data.jdbc.compiler.expr.AnyComparisonExpression;
import com.treasure_data.jdbc.compiler.expr.BinaryExpression;
import com.treasure_data.jdbc.compiler.expr.CaseExpression;
import com.treasure_data.jdbc.compiler.expr.DateValue;
import com.treasure_data.jdbc.compiler.expr.DoubleValue;
import com.treasure_data.jdbc.compiler.expr.Expression;
import com.treasure_data.jdbc.compiler.expr.ExpressionVisitor;
import com.treasure_data.jdbc.compiler.expr.Function;
import com.treasure_data.jdbc.compiler.expr.InverseExpression;
import com.treasure_data.jdbc.compiler.expr.JdbcParameter;
import com.treasure_data.jdbc.compiler.expr.LongValue;
import com.treasure_data.jdbc.compiler.expr.NullValue;
import com.treasure_data.jdbc.compiler.expr.Parenthesis;
import com.treasure_data.jdbc.compiler.expr.StringValue;
import com.treasure_data.jdbc.compiler.expr.TimeValue;
import com.treasure_data.jdbc.compiler.expr.TimestampValue;
import com.treasure_data.jdbc.compiler.expr.WhenClause;
import com.treasure_data.jdbc.compiler.expr.ops.Addition;
import com.treasure_data.jdbc.compiler.expr.ops.AndExpression;
import com.treasure_data.jdbc.compiler.expr.ops.Between;
import com.treasure_data.jdbc.compiler.expr.ops.BitwiseAnd;
import com.treasure_data.jdbc.compiler.expr.ops.BitwiseOr;
import com.treasure_data.jdbc.compiler.expr.ops.BitwiseXor;
import com.treasure_data.jdbc.compiler.expr.ops.Concat;
import com.treasure_data.jdbc.compiler.expr.ops.Division;
import com.treasure_data.jdbc.compiler.expr.ops.EqualsTo;
import com.treasure_data.jdbc.compiler.expr.ops.ExistsExpression;
import com.treasure_data.jdbc.compiler.expr.ops.ExpressionList;
import com.treasure_data.jdbc.compiler.expr.ops.GreaterThan;
import com.treasure_data.jdbc.compiler.expr.ops.GreaterThanEquals;
import com.treasure_data.jdbc.compiler.expr.ops.InExpression;
import com.treasure_data.jdbc.compiler.expr.ops.IsNullExpression;
import com.treasure_data.jdbc.compiler.expr.ops.ItemsListVisitor;
import com.treasure_data.jdbc.compiler.expr.ops.LikeExpression;
import com.treasure_data.jdbc.compiler.expr.ops.Matches;
import com.treasure_data.jdbc.compiler.expr.ops.MinorThan;
import com.treasure_data.jdbc.compiler.expr.ops.MinorThanEquals;
import com.treasure_data.jdbc.compiler.expr.ops.Multiplication;
import com.treasure_data.jdbc.compiler.expr.ops.NotEqualsTo;
import com.treasure_data.jdbc.compiler.expr.ops.OrExpression;
import com.treasure_data.jdbc.compiler.expr.ops.Subtraction;
import com.treasure_data.jdbc.compiler.schema.Column;
import com.treasure_data.jdbc.compiler.stat.SelectVisitor;
import com.treasure_data.jdbc.compiler.stat.SubSelect;

/**
 * A class to de-parse (that is, tranform from JSqlParser hierarchy into a
 * string) an {@link com.treasure_data.jdbc.compiler.expr.Expression}
 */
public class ExpressionDeParser implements ExpressionVisitor, ItemsListVisitor {

    protected StringBuilder buffer;
    protected SelectVisitor selectVisitor;
    protected boolean useBracketsInExprList = true;

    public ExpressionDeParser() {
    }

    /**
     * @param selectVisitor
     *            a SelectVisitor to de-parse SubSelects. It has to share the
     *            same<br>
     *            StringBuilder as this object in order to work, as:
     * 
     *            <pre>
     * <code>
     * StringBuilder myBuf = new StringBuilder();
     * MySelectDeparser selectDeparser = new  MySelectDeparser();
     * selectDeparser.setBuffer(myBuf);
     * ExpressionDeParser expressionDeParser = new ExpressionDeParser(selectDeparser, myBuf);
     * </code>
     * </pre>
     * @param buffer
     *            the buffer that will be filled with the expression
     */
    public ExpressionDeParser(SelectVisitor selectVisitor, StringBuilder buffer) {
        this.selectVisitor = selectVisitor;
        this.buffer = buffer;
    }

    public StringBuilder getBuffer() {
        return buffer;
    }

    public void setBuffer(StringBuilder buffer) {
        this.buffer = buffer;
    }

    public void visit(Addition addition) {
        visitBinaryExpression(addition, " + ");
    }

    public void visit(AndExpression andExpression) {
        visitBinaryExpression(andExpression, " AND ");
    }

    public void visit(Between between) {
        between.getLeftExpression().accept(this);
        if (between.isNot())
            buffer.append(" NOT");

        buffer.append(" BETWEEN ");
        between.getBetweenExpressionStart().accept(this);
        buffer.append(" AND ");
        between.getBetweenExpressionEnd().accept(this);

    }

    public void visit(Division division) {
        visitBinaryExpression(division, " / ");

    }

    public void visit(DoubleValue doubleValue) {
        buffer.append(doubleValue.toString());

    }

    public void visit(EqualsTo equalsTo) {
        visitBinaryExpression(equalsTo, " = ");
    }

    public void visit(GreaterThan greaterThan) {
        visitBinaryExpression(greaterThan, " > ");
    }

    public void visit(GreaterThanEquals greaterThanEquals) {
        visitBinaryExpression(greaterThanEquals, " >= ");

    }

    public void visit(InExpression inExpression) {

        inExpression.getLeftExpression().accept(this);
        if (inExpression.isNot())
            buffer.append(" NOT");
        buffer.append(" IN ");

        inExpression.getItemsList().accept(this);
    }

    public void visit(InverseExpression inverseExpression) {
        buffer.append("-");
        inverseExpression.getExpression().accept(this);
    }

    public void visit(IsNullExpression isNullExpression) {
        isNullExpression.getLeftExpression().accept(this);
        if (isNullExpression.isNot()) {
            buffer.append(" IS NOT NULL");
        } else {
            buffer.append(" IS NULL");
        }
    }

    public void visit(JdbcParameter jdbcParameter) {
        buffer.append("?");

    }

    public void visit(LikeExpression likeExpression) {
        visitBinaryExpression(likeExpression, " LIKE ");
        String escape = likeExpression.getEscape();
        if (escape != null) {
            buffer.append(" ESCAPE '").append(escape).append('\'');
        }
    }

    public void visit(ExistsExpression existsExpression) {
        if (existsExpression.isNot()) {
            buffer.append(" NOT EXISTS ");
        } else {
            buffer.append(" EXISTS ");
        }
        existsExpression.getRightExpression().accept(this);
    }

    public void visit(LongValue longValue) {
        buffer.append(longValue.getStringValue());

    }

    public void visit(MinorThan minorThan) {
        visitBinaryExpression(minorThan, " < ");

    }

    public void visit(MinorThanEquals minorThanEquals) {
        visitBinaryExpression(minorThanEquals, " <= ");

    }

    public void visit(Multiplication multiplication) {
        visitBinaryExpression(multiplication, " * ");

    }

    public void visit(NotEqualsTo notEqualsTo) {
        visitBinaryExpression(notEqualsTo, " <> ");

    }

    public void visit(NullValue nullValue) {
        buffer.append("NULL");

    }

    public void visit(OrExpression orExpression) {
        visitBinaryExpression(orExpression, " OR ");

    }

    public void visit(Parenthesis parenthesis) {
        if (parenthesis.isNot())
            buffer.append(" NOT ");

        buffer.append("(");
        parenthesis.getExpression().accept(this);
        buffer.append(")");

    }

    public void visit(StringValue stringValue) {
        buffer.append("'" + stringValue.getValue() + "'");

    }

    public void visit(Subtraction subtraction) {
        visitBinaryExpression(subtraction, "-");

    }

    private void visitBinaryExpression(BinaryExpression binaryExpression,
            String operator) {
        if (binaryExpression.isNot())
            buffer.append(" NOT ");
        binaryExpression.getLeftExpression().accept(this);
        buffer.append(operator);
        binaryExpression.getRightExpression().accept(this);

    }

    public void visit(SubSelect subSelect) {
        buffer.append("(");
        subSelect.getSelectBody().accept(selectVisitor);
        buffer.append(")");
    }

    public void visit(Column tableColumn) {
        String tableName = tableColumn.getTable().getAlias();
        if (tableName == null) {
            tableName = tableColumn.getTable().getWholeTableName();
        }
        if (tableName != null) {
            buffer.append(tableName + ".");
        }

        buffer.append(tableColumn.getColumnName());
    }

    public void visit(Function function) {
        if (function.isEscaped()) {
            buffer.append("{fn ");
        }

        buffer.append(function.getName());
        if (function.isAllColumns()) {
            buffer.append("(*)");
        } else if (function.getParameters() == null) {
            buffer.append("()");
        } else {
            boolean oldUseBracketsInExprList = useBracketsInExprList;
            if (function.isDistinct()) {
                useBracketsInExprList = false;
                buffer.append("(DISTINCT ");
            }
            visit(function.getParameters());
            useBracketsInExprList = oldUseBracketsInExprList;
            if (function.isDistinct()) {
                buffer.append(")");
            }
        }

        if (function.isEscaped()) {
            buffer.append("}");
        }

    }

    public void visit(ExpressionList expressionList) {
        if (useBracketsInExprList)
            buffer.append("(");
        for (Iterator<Expression> iter = expressionList.getExpressions()
                .iterator(); iter.hasNext();) {
            Expression expression = (Expression) iter.next();
            expression.accept(this);
            if (iter.hasNext())
                buffer.append(", ");
        }
        if (useBracketsInExprList)
            buffer.append(")");
    }

    public SelectVisitor getSelectVisitor() {
        return selectVisitor;
    }

    public void setSelectVisitor(SelectVisitor visitor) {
        selectVisitor = visitor;
    }

    public void visit(DateValue dateValue) {
        buffer.append("{d '" + dateValue.getValue().toString() + "'}");
    }

    public void visit(TimestampValue timestampValue) {
        buffer.append("{ts '" + timestampValue.getValue().toString() + "'}");
    }

    public void visit(TimeValue timeValue) {
        buffer.append("{t '" + timeValue.getValue().toString() + "'}");
    }

    public void visit(CaseExpression caseExpression) {
        buffer.append("CASE ");
        Expression switchExp = caseExpression.getSwitchExpression();
        if (switchExp != null) {
            switchExp.accept(this);
            buffer.append(" ");
        }

        for (Iterator<Expression> iter = caseExpression.getWhenClauses()
                .iterator(); iter.hasNext();) {
            Expression exp = (Expression) iter.next();
            exp.accept(this);
        }

        Expression elseExp = caseExpression.getElseExpression();
        if (elseExp != null) {
            buffer.append("ELSE ");
            elseExp.accept(this);
            buffer.append(" ");
        }

        buffer.append("END");
    }

    public void visit(WhenClause whenClause) {
        buffer.append("WHEN ");
        whenClause.getWhenExpression().accept(this);
        buffer.append(" THEN ");
        whenClause.getThenExpression().accept(this);
        buffer.append(" ");
    }

    public void visit(AllComparisonExpression allComparisonExpression) {
        buffer.append(" ALL ");
        allComparisonExpression.GetSubSelect().accept((ExpressionVisitor) this);
    }

    public void visit(AnyComparisonExpression anyComparisonExpression) {
        buffer.append(" ANY ");
        anyComparisonExpression.GetSubSelect().accept((ExpressionVisitor) this);
    }

    public void visit(Concat concat) {
        visitBinaryExpression(concat, " || ");
    }

    public void visit(Matches matches) {
        visitBinaryExpression(matches, " @@ ");
    }

    public void visit(BitwiseAnd bitwiseAnd) {
        visitBinaryExpression(bitwiseAnd, " & ");
    }

    public void visit(BitwiseOr bitwiseOr) {
        visitBinaryExpression(bitwiseOr, " | ");
    }

    public void visit(BitwiseXor bitwiseXor) {
        visitBinaryExpression(bitwiseXor, " ^ ");
    }

}
