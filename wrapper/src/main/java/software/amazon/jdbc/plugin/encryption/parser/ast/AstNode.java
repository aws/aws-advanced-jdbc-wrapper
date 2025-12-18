package software.amazon.jdbc.plugin.encryption.parser.ast;

/**
 * Base class for all AST nodes
 */
public abstract class AstNode {
    /**
     * Accept method for visitor pattern
     */
    public abstract <T> T accept(AstVisitor<T> visitor);
}
