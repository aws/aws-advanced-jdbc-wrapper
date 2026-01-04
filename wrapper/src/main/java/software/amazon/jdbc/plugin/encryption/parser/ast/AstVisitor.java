package software.amazon.jdbc.plugin.encryption.parser.ast;

/**
 * Visitor interface for AST traversal
 */
public interface AstVisitor<T> {
    T visit(SelectStatement node);
    T visit(InsertStatement node);
    T visit(UpdateStatement node);
    T visit(DeleteStatement node);
    T visit(CreateTableStatement node);
    T visit(BinaryExpression node);
    T visit(Identifier node);
    T visit(StringLiteral node);
    T visit(NumericLiteral node);
    T visit(Placeholder node);
    T visit(SubqueryExpression node);
    T visit(BooleanLiteral node);
}
