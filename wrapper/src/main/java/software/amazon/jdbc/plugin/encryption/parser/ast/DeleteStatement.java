package software.amazon.jdbc.plugin.encryption.parser.ast;

/**
 * DELETE statement
 */
public class DeleteStatement extends Statement {
    private final TableReference table;
    private final Expression whereClause;

    public DeleteStatement(TableReference table, Expression whereClause) {
        this.table = table;
        this.whereClause = whereClause;
    }

    public TableReference getTable() { return table; }
    public Expression getWhereClause() { return whereClause; }

    @Override
    public <T> T accept(AstVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
