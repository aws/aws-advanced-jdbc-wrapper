package software.amazon.jdbc.plugin.encryption.parser.ast;

import java.util.List;

/**
 * CREATE TABLE statement
 */
public class CreateTableStatement extends Statement {
    private final Identifier tableName;
    private final List<ColumnDefinition> columns;

    public CreateTableStatement(Identifier tableName, List<ColumnDefinition> columns) {
        this.tableName = tableName;
        this.columns = columns;
    }

    public Identifier getTableName() { return tableName; }
    public List<ColumnDefinition> getColumns() { return columns; }

    @Override
    public <T> T accept(AstVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
