package software.amazon.jdbc.plugin.encryption.parser.ast;

import java.util.List;

/**
 * INSERT statement
 */
public class InsertStatement extends Statement {
    private final TableReference table;
    private final List<Identifier> columns;
    private final List<List<Expression>> values;

    public InsertStatement(TableReference table, List<Identifier> columns, List<List<Expression>> values) {
        this.table = table;
        this.columns = columns;
        this.values = values;
    }

    public TableReference getTable() { return table; }
    public List<Identifier> getColumns() { return columns; }
    public List<List<Expression>> getValues() { return values; }

    @Override
    public <T> T accept(AstVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
