package software.amazon.jdbc.plugin.encryption.parser.ast;

/**
 * Assignment in UPDATE statement
 */
public class Assignment extends AstNode {
    private final Identifier column;
    private final Expression value;

    public Assignment(Identifier column, Expression value) {
        this.column = column;
        this.value = value;
    }

    public Identifier getColumn() { return column; }
    public Expression getValue() { return value; }

    @Override
    public <T> T accept(AstVisitor<T> visitor) {
        // Assignment doesn't have a visitor method, so we delegate to the value
        return value.accept(visitor);
    }
}
