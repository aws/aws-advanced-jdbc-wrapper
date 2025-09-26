package software.amazon.jdbc.plugin.encryption.parser.ast;

/**
 * Column definition in CREATE TABLE
 */
public class ColumnDefinition extends AstNode {
    private final Identifier columnName;
    private final String dataType;
    private final boolean notNull;
    private final boolean primaryKey;

    public ColumnDefinition(Identifier columnName, String dataType, boolean notNull, boolean primaryKey) {
        this.columnName = columnName;
        this.dataType = dataType;
        this.notNull = notNull;
        this.primaryKey = primaryKey;
    }

    public Identifier getColumnName() { return columnName; }
    public String getDataType() { return dataType; }
    public boolean isNotNull() { return notNull; }
    public boolean isPrimaryKey() { return primaryKey; }

    @Override
    public <T> T accept(AstVisitor<T> visitor) {
        // ColumnDefinition doesn't have a visitor method, so we delegate to the column name
        return columnName.accept(visitor);
    }
}
