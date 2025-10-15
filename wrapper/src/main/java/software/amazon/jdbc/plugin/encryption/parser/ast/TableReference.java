package software.amazon.jdbc.plugin.encryption.parser.ast;

/**
 * Table reference
 */
public class TableReference extends AstNode {
    private final Identifier tableName;
    private final String alias;

    public TableReference(Identifier tableName, String alias) {
        this.tableName = tableName;
        this.alias = alias;
    }

    public Identifier getTableName() { return tableName; }
    public String getAlias() { return alias; }

    @Override
    public <T> T accept(AstVisitor<T> visitor) {
        // TableReference doesn't have a visitor method, so we delegate to the table name
        return tableName.accept(visitor);
    }
}
