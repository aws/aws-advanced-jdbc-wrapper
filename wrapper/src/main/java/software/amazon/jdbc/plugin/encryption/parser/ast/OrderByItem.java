package software.amazon.jdbc.plugin.encryption.parser.ast;

/**
 * ORDER BY item
 */
public class OrderByItem extends AstNode {
    public enum Direction { ASC, DESC }

    private final Expression expression;
    private final Direction direction;

    public OrderByItem(Expression expression, Direction direction) {
        this.expression = expression;
        this.direction = direction;
    }

    public Expression getExpression() { return expression; }
    public Direction getDirection() { return direction; }

    @Override
    public <T> T accept(AstVisitor<T> visitor) {
        // OrderByItem doesn't have a visitor method, so we delegate to the expression
        return expression.accept(visitor);
    }
}
