package software.amazon.jdbc.plugin.encryption.wrapper;

import software.amazon.jdbc.plugin.encryption.KmsEncryptionPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * A Statement wrapper that provides transparent encryption/decryption functionality.
 * This wrapper intercepts SQL execution methods and wraps result sets with decryption support.
 * Note: Statement-based encryption is limited compared to PreparedStatement encryption.
 */
public class EncryptingStatement implements Statement {

    private static final Logger logger = LoggerFactory.getLogger(EncryptingStatement.class);

    private final Statement delegate;
    private final KmsEncryptionPlugin encryptionPlugin;

    /**
     * Creates an encrypting statement wrapper.
     *
     * @param delegate The underlying Statement to wrap
     * @param encryptionPlugin The encryption plugin to use
     */
    public EncryptingStatement(Statement delegate, KmsEncryptionPlugin encryptionPlugin) {
        this.delegate = delegate;
        this.encryptionPlugin = encryptionPlugin;

        logger.debug("Created EncryptingStatement wrapper");
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        logger.debug("Executing query with encryption support: {}", sql);

        ResultSet resultSet = delegate.executeQuery(sql);
        return encryptionPlugin.wrapResultSet(resultSet);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        logger.debug("Executing update with encryption support: {}", sql);

        // For Statement-based updates, we can't easily encrypt embedded values
        // This is a limitation - PreparedStatement should be used for full encryption support
        return delegate.executeUpdate(sql);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        logger.debug("Executing statement with encryption support: {}", sql);

        return delegate.execute(sql);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        ResultSet resultSet = delegate.getResultSet();
        if (resultSet != null) {
            return encryptionPlugin.wrapResultSet(resultSet);
        }
        return null;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        ResultSet resultSet = delegate.getGeneratedKeys();
        if (resultSet != null) {
            return encryptionPlugin.wrapResultSet(resultSet);
        }
        return null;
    }

    // All other Statement methods delegate directly to the wrapped statement

    @Override
    public void close() throws SQLException {
        delegate.close();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return delegate.getMaxFieldSize();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        delegate.setMaxFieldSize(max);
    }

    @Override
    public int getMaxRows() throws SQLException {
        return delegate.getMaxRows();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        delegate.setMaxRows(max);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        delegate.setEscapeProcessing(enable);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return delegate.getQueryTimeout();
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        delegate.setQueryTimeout(seconds);
    }

    @Override
    public void cancel() throws SQLException {
        delegate.cancel();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return delegate.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        delegate.clearWarnings();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        delegate.setCursorName(name);
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return delegate.getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return delegate.getMoreResults();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        delegate.setFetchDirection(direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return delegate.getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        delegate.setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return delegate.getFetchSize();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return delegate.getResultSetConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
        return delegate.getResultSetType();
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        delegate.addBatch(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        delegate.clearBatch();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return delegate.executeBatch();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return delegate.getConnection();
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return delegate.getMoreResults(current);
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return delegate.executeUpdate(sql, autoGeneratedKeys);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return delegate.executeUpdate(sql, columnIndexes);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return delegate.executeUpdate(sql, columnNames);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return delegate.execute(sql, autoGeneratedKeys);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return delegate.execute(sql, columnIndexes);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return delegate.execute(sql, columnNames);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return delegate.getResultSetHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return delegate.isClosed();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        delegate.setPoolable(poolable);
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return delegate.isPoolable();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        delegate.closeOnCompletion();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return delegate.isCloseOnCompletion();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        return delegate.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass()) || delegate.isWrapperFor(iface);
    }

    /**
     * Gets the underlying Statement.
     *
     * @return The wrapped Statement
     */
    public Statement getDelegate() {
        return delegate;
    }

    /**
     * Gets the encryption plugin instance.
     *
     * @return The KmsEncryptionPlugin instance
     */
    public KmsEncryptionPlugin getEncryptionPlugin() {
        return encryptionPlugin;
    }
}
