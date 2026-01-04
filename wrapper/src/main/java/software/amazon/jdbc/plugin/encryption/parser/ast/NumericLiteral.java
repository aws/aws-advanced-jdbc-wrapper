package software.amazon.jdbc.plugin.encryption.parser.ast;

/**
 * Numeric literal
 */
public class NumericLiteral extends Expression {
    private final String value;
    private final boolean isInteger;

    public NumericLiteral(String value, boolean isInteger) {
        this.value = value;
        this.isInteger = isInteger;
    }

    public String getValue() { return value; }
    public boolean isInteger() { return isInteger; }

    @Override
    public <T> T accept(AstVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
