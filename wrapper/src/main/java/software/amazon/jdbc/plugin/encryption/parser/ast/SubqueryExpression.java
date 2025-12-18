package software.amazon.jdbc.plugin.encryption.parser.ast;

/**
 * Represents a subquery expression in SQL
 */
public class SubqueryExpression extends Expression {
    private final SelectStatement selectStatement;

    public SubqueryExpression(SelectStatement selectStatement) {
        this.selectStatement = selectStatement;
    }

    public SelectStatement getSelectStatement() {
        return selectStatement;
    }

    @Override
    public <T> T accept(AstVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String toString() {
        return "(" + selectStatement.toString() + ")";
    }
}
