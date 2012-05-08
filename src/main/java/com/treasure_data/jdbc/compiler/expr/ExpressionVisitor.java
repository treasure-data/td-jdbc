package com.treasure_data.jdbc.compiler.expr;

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
import com.treasure_data.jdbc.compiler.expr.ops.GreaterThan;
import com.treasure_data.jdbc.compiler.expr.ops.GreaterThanEquals;
import com.treasure_data.jdbc.compiler.expr.ops.InExpression;
import com.treasure_data.jdbc.compiler.expr.ops.IsNullExpression;
import com.treasure_data.jdbc.compiler.expr.ops.LikeExpression;
import com.treasure_data.jdbc.compiler.expr.ops.Matches;
import com.treasure_data.jdbc.compiler.expr.ops.MinorThan;
import com.treasure_data.jdbc.compiler.expr.ops.MinorThanEquals;
import com.treasure_data.jdbc.compiler.expr.ops.Multiplication;
import com.treasure_data.jdbc.compiler.expr.ops.NotEqualsTo;
import com.treasure_data.jdbc.compiler.expr.ops.OrExpression;
import com.treasure_data.jdbc.compiler.expr.ops.Subtraction;
import com.treasure_data.jdbc.compiler.schema.Column;
import com.treasure_data.jdbc.compiler.stat.SubSelect;

public interface ExpressionVisitor {
    public void visit(NullValue nullValue);

    public void visit(Function function);

    public void visit(InverseExpression inverseExpression);

    public void visit(JdbcParameter jdbcParameter);

    public void visit(DoubleValue doubleValue);

    public void visit(LongValue longValue);

    public void visit(DateValue dateValue);

    public void visit(TimeValue timeValue);

    public void visit(TimestampValue timestampValue);

    public void visit(Parenthesis parenthesis);

    public void visit(StringValue stringValue);

    public void visit(Addition addition);

    public void visit(Division division);

    public void visit(Multiplication multiplication);

    public void visit(Subtraction subtraction);

    public void visit(AndExpression andExpression);

    public void visit(OrExpression orExpression);

    public void visit(Between between);

    public void visit(EqualsTo equalsTo);

    public void visit(GreaterThan greaterThan);

    public void visit(GreaterThanEquals greaterThanEquals);

    public void visit(InExpression inExpression);

    public void visit(IsNullExpression isNullExpression);

    public void visit(LikeExpression likeExpression);

    public void visit(MinorThan minorThan);

    public void visit(MinorThanEquals minorThanEquals);

    public void visit(NotEqualsTo notEqualsTo);

    public void visit(Column tableColumn);

    public void visit(SubSelect subSelect);

    public void visit(CaseExpression caseExpression);

    public void visit(WhenClause whenClause);

    public void visit(ExistsExpression existsExpression);

    public void visit(AllComparisonExpression allComparisonExpression);

    public void visit(AnyComparisonExpression anyComparisonExpression);

    public void visit(Concat concat);

    public void visit(Matches matches);

    public void visit(BitwiseAnd bitwiseAnd);

    public void visit(BitwiseOr bitwiseOr);

    public void visit(BitwiseXor bitwiseXor);

}
