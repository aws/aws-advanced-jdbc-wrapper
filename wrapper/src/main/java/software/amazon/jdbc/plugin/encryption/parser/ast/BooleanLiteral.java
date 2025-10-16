package software.amazon.jdbc.plugin.encryption.parser.ast;

/**
 * Represents a boolean literal (TRUE/FALSE) in SQL
 */
public class BooleanLiteral extends Expression {
    private final boolean value;

    public BooleanLiteral(boolean value) {
        this.value = value;
    }

    public boolean getValue() {
        return value;
    }

    @Override
    public <T> T accept(AstVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String toString() {
        return String.valueOf(value).toUpperCase();
    }
}
