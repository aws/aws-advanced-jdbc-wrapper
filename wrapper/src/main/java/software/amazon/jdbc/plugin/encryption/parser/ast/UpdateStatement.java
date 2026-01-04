package software.amazon.jdbc.plugin.encryption.parser.ast;

import java.util.List;

/**
 * UPDATE statement
 */
public class UpdateStatement extends Statement {
    private final TableReference table;
    private final List<Assignment> assignments;
    private final Expression whereClause;

    public UpdateStatement(TableReference table, List<Assignment> assignments, Expression whereClause) {
        this.table = table;
        this.assignments = assignments;
        this.whereClause = whereClause;
    }

    public TableReference getTable() { return table; }
    public List<Assignment> getAssignments() { return assignments; }
    public Expression getWhereClause() { return whereClause; }

    @Override
    public <T> T accept(AstVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
