package software.amazon.jdbc.plugin.encryption.parser.ast;

/**
 * SELECT item (column or expression in SELECT clause)
 */
public class SelectItem extends AstNode {
    private final Expression expression;
    private final String alias;

    public SelectItem(Expression expression, String alias) {
        this.expression = expression;
        this.alias = alias;
    }

    public Expression getExpression() { return expression; }
    public String getAlias() { return alias; }

    @Override
    public <T> T accept(AstVisitor<T> visitor) {
        // SelectItem doesn't have a visitor method, so we delegate to the expression
        return expression.accept(visitor);
    }
}
