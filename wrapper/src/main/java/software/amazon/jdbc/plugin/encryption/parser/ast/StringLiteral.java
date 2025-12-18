package software.amazon.jdbc.plugin.encryption.parser.ast;

/**
 * String literal
 */
public class StringLiteral extends Expression {
    private final String value;

    public StringLiteral(String value) {
        this.value = value;
    }

    public String getValue() { return value; }

    @Override
    public <T> T accept(AstVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
