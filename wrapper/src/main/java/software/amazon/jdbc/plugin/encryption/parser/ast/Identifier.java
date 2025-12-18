package software.amazon.jdbc.plugin.encryption.parser.ast;

/**
 * Identifier (table name, column name, etc.)
 */
public class Identifier extends Expression {
    private final String name;
    private final String schema;

    public Identifier(String name) {
        this(null, name);
    }

    public Identifier(String schema, String name) {
        this.schema = schema;
        this.name = name;
    }

    public String getName() { return name; }
    public String getSchema() { return schema; }
    public String getFullName() {
        return schema != null ? schema + "." + name : name;
    }

    @Override
    public <T> T accept(AstVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
