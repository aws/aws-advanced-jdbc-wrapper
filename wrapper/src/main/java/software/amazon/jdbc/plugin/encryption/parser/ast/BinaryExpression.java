package software.amazon.jdbc.plugin.encryption.parser.ast;

/**
 * Binary expression (e.g., a = b, x + y)
 */
public class BinaryExpression extends Expression {
    public enum Operator {
        EQUALS, NOT_EQUALS, LESS_THAN, GREATER_THAN, LESS_EQUALS, GREATER_EQUALS,
        PLUS, MINUS, MULTIPLY, DIVIDE, MODULO,
        AND, OR, LIKE, IN, BETWEEN
    }

    private final Expression left;
    private final Operator operator;
    private final Expression right;

    public BinaryExpression(Expression left, Operator operator, Expression right) {
        this.left = left;
        this.operator = operator;
        this.right = right;
    }

    public Expression getLeft() { return left; }
    public Operator getOperator() { return operator; }
    public Expression getRight() { return right; }

    @Override
    public <T> T accept(AstVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
