package software.amazon.jdbc.plugin.encryption.parser.ast;

/**
 * JDBC placeholder (?)
 */
public class Placeholder extends Expression {

    public Placeholder() {
    }

    @Override
    public <T> T accept(AstVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String toString() {
        return "?";
    }
}
