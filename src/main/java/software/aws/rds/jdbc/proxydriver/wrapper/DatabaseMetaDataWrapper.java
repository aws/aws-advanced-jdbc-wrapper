package software.aws.rds.jdbc.proxydriver.wrapper;

import software.aws.rds.jdbc.proxydriver.ConnectionPluginManager;
import software.aws.rds.jdbc.proxydriver.util.WrapperUtils;

import java.sql.*;

public class DatabaseMetaDataWrapper implements DatabaseMetaData {

    protected DatabaseMetaData databaseMetaData;
    protected Class<?> databaseMetaDataClass;
    protected ConnectionPluginManager pluginManager;

    public DatabaseMetaDataWrapper(DatabaseMetaData databaseMetaData, ConnectionPluginManager pluginManager) {
        if (databaseMetaData == null) {
            throw new IllegalArgumentException("databaseMetaData");
        }
        if (pluginManager == null) {
            throw new IllegalArgumentException("pluginManager");
        }

        this.databaseMetaData = databaseMetaData;
        this.databaseMetaDataClass = this.databaseMetaData.getClass();
        this.pluginManager = pluginManager;
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.allProceduresAreCallable",
                () -> this.databaseMetaData.allProceduresAreCallable());
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.allTablesAreSelectable",
                () -> this.databaseMetaData.allTablesAreSelectable());
    }

    @Override
    public String getURL() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getURL",
                () -> this.databaseMetaData.getURL());
    }

    @Override
    public String getUserName() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getUserName",
                () -> this.databaseMetaData.getUserName());
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.isReadOnly",
                () -> this.databaseMetaData.isReadOnly());
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.nullsAreSortedHigh",
                () -> this.databaseMetaData.nullsAreSortedHigh());
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.nullsAreSortedLow",
                () -> this.databaseMetaData.nullsAreSortedLow());
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.nullsAreSortedAtStart",
                () -> this.databaseMetaData.nullsAreSortedAtStart());
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.nullsAreSortedAtEnd",
                () -> this.databaseMetaData.nullsAreSortedAtEnd());
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getDatabaseProductName",
                () -> this.databaseMetaData.getDatabaseProductName());
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getDatabaseProductVersion",
                () -> this.databaseMetaData.getDatabaseProductVersion());
    }

    @Override
    public String getDriverName() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getDriverName",
                () -> this.databaseMetaData.getDriverName());
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getDriverVersion",
                () -> this.databaseMetaData.getDriverVersion());
    }

    @Override
    public int getDriverMajorVersion() {
        return WrapperUtils.executeWithPlugins(
                int.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getDriverMajorVersion",
                () -> this.databaseMetaData.getDriverMajorVersion());
    }

    @Override
    public int getDriverMinorVersion() {
        return WrapperUtils.executeWithPlugins(
                int.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getDriverMinorVersion",
                () -> this.databaseMetaData.getDriverMinorVersion());
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.usesLocalFiles",
                () -> this.databaseMetaData.usesLocalFiles());
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.usesLocalFilePerTable",
                () -> this.databaseMetaData.usesLocalFilePerTable());
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsMixedCaseIdentifiers",
                () -> this.databaseMetaData.supportsMixedCaseIdentifiers());
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.storesUpperCaseIdentifiers",
                () -> this.databaseMetaData.storesUpperCaseIdentifiers());
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.storesLowerCaseIdentifiers",
                () -> this.databaseMetaData.storesLowerCaseIdentifiers());
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.storesMixedCaseIdentifiers",
                () -> this.databaseMetaData.storesMixedCaseIdentifiers());
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsMixedCaseQuotedIdentifiers",
                () -> this.databaseMetaData.supportsMixedCaseQuotedIdentifiers());
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.storesUpperCaseQuotedIdentifiers",
                () -> this.databaseMetaData.storesUpperCaseQuotedIdentifiers());
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.storesLowerCaseQuotedIdentifiers",
                () -> this.databaseMetaData.storesLowerCaseQuotedIdentifiers());
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.storesMixedCaseQuotedIdentifiers",
                () -> this.databaseMetaData.storesMixedCaseQuotedIdentifiers());
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getIdentifierQuoteString",
                () -> this.databaseMetaData.getIdentifierQuoteString());
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getSQLKeywords",
                () -> this.databaseMetaData.getSQLKeywords());
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getNumericFunctions",
                () -> this.databaseMetaData.getNumericFunctions());
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getStringFunctions",
                () -> this.databaseMetaData.getStringFunctions());
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getSystemFunctions",
                () -> this.databaseMetaData.getSystemFunctions());
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getTimeDateFunctions",
                () -> this.databaseMetaData.getTimeDateFunctions());
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getSearchStringEscape",
                () -> this.databaseMetaData.getSearchStringEscape());
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getExtraNameCharacters",
                () -> this.databaseMetaData.getExtraNameCharacters());
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsAlterTableWithAddColumn",
                () -> this.databaseMetaData.supportsAlterTableWithAddColumn());
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsAlterTableWithDropColumn",
                () -> this.databaseMetaData.supportsAlterTableWithDropColumn());
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsColumnAliasing",
                () -> this.databaseMetaData.supportsColumnAliasing());
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.nullPlusNonNullIsNull",
                () -> this.databaseMetaData.nullPlusNonNullIsNull());
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsConvert",
                () -> this.databaseMetaData.supportsConvert());
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsConvert",
                () -> this.databaseMetaData.supportsConvert(fromType, toType),
                fromType, toType);
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsTableCorrelationNames",
                () -> this.databaseMetaData.supportsTableCorrelationNames());
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsDifferentTableCorrelationNames",
                () -> this.databaseMetaData.supportsDifferentTableCorrelationNames());
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsExpressionsInOrderBy",
                () -> this.databaseMetaData.supportsExpressionsInOrderBy());
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsOrderByUnrelated",
                () -> this.databaseMetaData.supportsOrderByUnrelated());
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsGroupBy",
                () -> this.databaseMetaData.supportsGroupBy());
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsGroupByUnrelated",
                () -> this.databaseMetaData.supportsGroupByUnrelated());
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsGroupByBeyondSelect",
                () -> this.databaseMetaData.supportsGroupByBeyondSelect());
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsLikeEscapeClause",
                () -> this.databaseMetaData.supportsLikeEscapeClause());
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsMultipleResultSets",
                () -> this.databaseMetaData.supportsMultipleResultSets());
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsMultipleTransactions",
                () -> this.databaseMetaData.supportsMultipleTransactions());
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsNonNullableColumns",
                () -> this.databaseMetaData.supportsNonNullableColumns());
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsMinimumSQLGrammar",
                () -> this.databaseMetaData.supportsMinimumSQLGrammar());
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsCoreSQLGrammar",
                () -> this.databaseMetaData.supportsCoreSQLGrammar());
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsExtendedSQLGrammar",
                () -> this.databaseMetaData.supportsExtendedSQLGrammar());
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsANSI92EntryLevelSQL",
                () -> this.databaseMetaData.supportsANSI92EntryLevelSQL());
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsANSI92IntermediateSQL",
                () -> this.databaseMetaData.supportsANSI92IntermediateSQL());
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsANSI92FullSQL",
                () -> this.databaseMetaData.supportsANSI92FullSQL());
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsIntegrityEnhancementFacility",
                () -> this.databaseMetaData.supportsIntegrityEnhancementFacility());
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsOuterJoins",
                () -> this.databaseMetaData.supportsOuterJoins());
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsFullOuterJoins",
                () -> this.databaseMetaData.supportsFullOuterJoins());
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsLimitedOuterJoins",
                () -> this.databaseMetaData.supportsLimitedOuterJoins());
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getSchemaTerm",
                () -> this.databaseMetaData.getSchemaTerm());
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getProcedureTerm",
                () -> this.databaseMetaData.getProcedureTerm());
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getCatalogTerm",
                () -> this.databaseMetaData.getCatalogTerm());
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.isCatalogAtStart",
                () -> this.databaseMetaData.isCatalogAtStart());
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getCatalogSeparator",
                () -> this.databaseMetaData.getCatalogSeparator());
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsSchemasInDataManipulation",
                () -> this.databaseMetaData.supportsSchemasInDataManipulation());
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsSchemasInProcedureCalls",
                () -> this.databaseMetaData.supportsSchemasInProcedureCalls());
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsSchemasInTableDefinitions",
                () -> this.databaseMetaData.supportsSchemasInTableDefinitions());
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsSchemasInIndexDefinitions",
                () -> this.databaseMetaData.supportsSchemasInIndexDefinitions());
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsSchemasInPrivilegeDefinitions",
                () -> this.databaseMetaData.supportsSchemasInPrivilegeDefinitions());
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsCatalogsInDataManipulation",
                () -> this.databaseMetaData.supportsCatalogsInDataManipulation());
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsCatalogsInProcedureCalls",
                () -> this.databaseMetaData.supportsCatalogsInProcedureCalls());
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsCatalogsInTableDefinitions",
                () -> this.databaseMetaData.supportsCatalogsInTableDefinitions());
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsCatalogsInIndexDefinitions",
                () -> this.databaseMetaData.supportsCatalogsInIndexDefinitions());
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsCatalogsInPrivilegeDefinitions",
                () -> this.databaseMetaData.supportsCatalogsInPrivilegeDefinitions());
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsPositionedDelete",
                () -> this.databaseMetaData.supportsPositionedDelete());
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsPositionedUpdate",
                () -> this.databaseMetaData.supportsPositionedUpdate());
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsSelectForUpdate",
                () -> this.databaseMetaData.supportsSelectForUpdate());
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsStoredProcedures",
                () -> this.databaseMetaData.supportsStoredProcedures());
    }

    @SuppressWarnings("SpellCheckingInspection")
    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsSubqueriesInComparisons",
                () -> this.databaseMetaData.supportsSubqueriesInComparisons());
    }

    @SuppressWarnings("SpellCheckingInspection")
    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsSubqueriesInExists",
                () -> this.databaseMetaData.supportsSubqueriesInExists());
    }

    @SuppressWarnings("SpellCheckingInspection")
    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsSubqueriesInIns",
                () -> this.databaseMetaData.supportsSubqueriesInIns());
    }

    @SuppressWarnings("SpellCheckingInspection")
    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsSubqueriesInQuantifieds",
                () -> this.databaseMetaData.supportsSubqueriesInQuantifieds());
    }

    @SuppressWarnings("SpellCheckingInspection")
    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsCorrelatedSubqueries",
                () -> this.databaseMetaData.supportsCorrelatedSubqueries());
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsUnion",
                () -> this.databaseMetaData.supportsUnion());
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsUnionAll",
                () -> this.databaseMetaData.supportsUnionAll());
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsOpenCursorsAcrossCommit",
                () -> this.databaseMetaData.supportsOpenCursorsAcrossCommit());
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsOpenCursorsAcrossRollback",
                () -> this.databaseMetaData.supportsOpenCursorsAcrossRollback());
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsOpenStatementsAcrossCommit",
                () -> this.databaseMetaData.supportsOpenStatementsAcrossCommit());
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsOpenStatementsAcrossRollback",
                () -> this.databaseMetaData.supportsOpenStatementsAcrossRollback());
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getMaxBinaryLiteralLength",
                () -> this.databaseMetaData.getMaxBinaryLiteralLength());
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getMaxCharLiteralLength",
                () -> this.databaseMetaData.getMaxCharLiteralLength());
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getMaxColumnNameLength",
                () -> this.databaseMetaData.getMaxColumnNameLength());
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getMaxColumnsInGroupBy",
                () -> this.databaseMetaData.getMaxColumnsInGroupBy());
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getMaxColumnsInIndex",
                () -> this.databaseMetaData.getMaxColumnsInIndex());
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getMaxColumnsInOrderBy",
                () -> this.databaseMetaData.getMaxColumnsInOrderBy());
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getMaxColumnsInSelect",
                () -> this.databaseMetaData.getMaxColumnsInSelect());
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getMaxColumnsInTable",
                () -> this.databaseMetaData.getMaxColumnsInTable());
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getMaxConnections",
                () -> this.databaseMetaData.getMaxConnections());
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getMaxCursorNameLength",
                () -> this.databaseMetaData.getMaxCursorNameLength());
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getMaxIndexLength",
                () -> this.databaseMetaData.getMaxIndexLength());
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getMaxSchemaNameLength",
                () -> this.databaseMetaData.getMaxSchemaNameLength());
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getMaxProcedureNameLength",
                () -> this.databaseMetaData.getMaxProcedureNameLength());
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getMaxCatalogNameLength",
                () -> this.databaseMetaData.getMaxCatalogNameLength());
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getMaxRowSize",
                () -> this.databaseMetaData.getMaxRowSize());
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.doesMaxRowSizeIncludeBlobs",
                () -> this.databaseMetaData.doesMaxRowSizeIncludeBlobs());
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getMaxStatementLength",
                () -> this.databaseMetaData.getMaxStatementLength());
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getMaxStatements",
                () -> this.databaseMetaData.getMaxStatements());
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getMaxTableNameLength",
                () -> this.databaseMetaData.getMaxTableNameLength());
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getMaxTablesInSelect",
                () -> this.databaseMetaData.getMaxTablesInSelect());
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getMaxUserNameLength",
                () -> this.databaseMetaData.getMaxUserNameLength());
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getDefaultTransactionIsolation",
                () -> this.databaseMetaData.getDefaultTransactionIsolation());
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsTransactions",
                () -> this.databaseMetaData.supportsTransactions());
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsTransactionIsolationLevel",
                () -> this.databaseMetaData.supportsTransactionIsolationLevel(level),
                level);
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsDataDefinitionAndDataManipulationTransactions",
                () -> this.databaseMetaData.supportsDataDefinitionAndDataManipulationTransactions());
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsDataManipulationTransactionsOnly",
                () -> this.databaseMetaData.supportsDataManipulationTransactionsOnly());
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.dataDefinitionCausesTransactionCommit",
                () -> this.databaseMetaData.dataDefinitionCausesTransactionCommit());
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.dataDefinitionIgnoredInTransactions",
                () -> this.databaseMetaData.dataDefinitionIgnoredInTransactions());
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                ResultSet.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getProcedures",
                () -> this.databaseMetaData.getProcedures(catalog, schemaPattern, procedureNamePattern),
                catalog, schemaPattern, procedureNamePattern);
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                ResultSet.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getProcedureColumns",
                () -> this.databaseMetaData.getProcedureColumns(catalog, schemaPattern, procedureNamePattern, columnNamePattern),
                catalog, schemaPattern, procedureNamePattern, columnNamePattern);
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                ResultSet.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getTables",
                () -> this.databaseMetaData.getTables(catalog, schemaPattern, tableNamePattern, types),
                catalog, schemaPattern, tableNamePattern, types);
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                ResultSet.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getSchemas",
                () -> this.databaseMetaData.getSchemas());
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                ResultSet.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getCatalogs",
                () -> this.databaseMetaData.getCatalogs());
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                ResultSet.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getTableTypes",
                () -> this.databaseMetaData.getTableTypes());
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                ResultSet.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getColumns",
                () -> this.databaseMetaData.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern),
                catalog, schemaPattern, tableNamePattern, columnNamePattern);
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                ResultSet.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getColumnPrivileges",
                () -> this.databaseMetaData.getColumnPrivileges(catalog, schema, table, columnNamePattern),
                catalog, schema, table, columnNamePattern);
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                ResultSet.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getTablePrivileges",
                () -> this.databaseMetaData.getTablePrivileges(catalog, schemaPattern, tableNamePattern),
                catalog, schemaPattern, tableNamePattern);
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                ResultSet.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getBestRowIdentifier",
                () -> this.databaseMetaData.getBestRowIdentifier(catalog, schema, table, scope, nullable),
                catalog, schema, table, scope, nullable);
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                ResultSet.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getVersionColumns",
                () -> this.databaseMetaData.getVersionColumns(catalog, schema, table),
                catalog, schema, table);
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                ResultSet.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getPrimaryKeys",
                () -> this.databaseMetaData.getPrimaryKeys(catalog, schema, table),
                catalog, schema, table);
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                ResultSet.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getImportedKeys",
                () -> this.databaseMetaData.getImportedKeys(catalog, schema, table),
                catalog, schema, table);
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                ResultSet.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getExportedKeys",
                () -> this.databaseMetaData.getExportedKeys(catalog, schema, table),
                catalog, schema, table);
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
                                       String foreignCatalog, String foreignSchema, String foreignTable)
            throws SQLException {
        return WrapperUtils.executeWithPlugins(
                ResultSet.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getCrossReference",
                () -> this.databaseMetaData.getCrossReference(
                        parentCatalog, parentSchema, parentTable, foreignCatalog, foreignSchema, foreignTable),
                parentCatalog, parentSchema, parentTable, foreignCatalog, foreignSchema, foreignTable);
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                ResultSet.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getTypeInfo",
                () -> this.databaseMetaData.getTypeInfo());
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
            throws SQLException {
        return WrapperUtils.executeWithPlugins(
                ResultSet.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getIndexInfo",
                () -> this.databaseMetaData.getIndexInfo(catalog, schema, table, unique, approximate),
                catalog, schema, table, unique, approximate);
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsResultSetType",
                () -> this.databaseMetaData.supportsResultSetType(type),
                type);
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsResultSetConcurrency",
                () -> this.databaseMetaData.supportsResultSetConcurrency(type, concurrency),
                type, concurrency);
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.ownUpdatesAreVisible",
                () -> this.databaseMetaData.ownUpdatesAreVisible(type),
                type);
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.ownDeletesAreVisible",
                () -> this.databaseMetaData.ownDeletesAreVisible(type),
                type);
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.ownInsertsAreVisible",
                () -> this.databaseMetaData.ownInsertsAreVisible(type),
                type);
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.othersUpdatesAreVisible",
                () -> this.databaseMetaData.othersUpdatesAreVisible(type),
                type);
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.othersDeletesAreVisible",
                () -> this.databaseMetaData.othersDeletesAreVisible(type),
                type);
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.othersInsertsAreVisible",
                () -> this.databaseMetaData.othersInsertsAreVisible(type),
                type);
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.updatesAreDetected",
                () -> this.databaseMetaData.updatesAreDetected(type),
                type);
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.deletesAreDetected",
                () -> this.databaseMetaData.deletesAreDetected(type),
                type);
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.insertsAreDetected",
                () -> this.databaseMetaData.insertsAreDetected(type),
                type);
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsBatchUpdates",
                () -> this.databaseMetaData.supportsBatchUpdates());
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
            throws SQLException {
        return WrapperUtils.executeWithPlugins(
                ResultSet.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getUDTs",
                () -> this.databaseMetaData.getUDTs(catalog, schemaPattern, typeNamePattern, types),
                catalog, schemaPattern, typeNamePattern, types);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                Connection.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getConnection",
                () -> this.databaseMetaData.getConnection());
    }

    @SuppressWarnings("SpellCheckingInspection")
    @Override
    public boolean supportsSavepoints() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsSavepoints",
                () -> this.databaseMetaData.supportsSavepoints());
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsNamedParameters",
                () -> this.databaseMetaData.supportsNamedParameters());
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsMultipleOpenResults",
                () -> this.databaseMetaData.supportsMultipleOpenResults());
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsGetGeneratedKeys",
                () -> this.databaseMetaData.supportsGetGeneratedKeys());
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                ResultSet.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getSuperTypes",
                () -> this.databaseMetaData.getSuperTypes(catalog, schemaPattern, typeNamePattern),
                catalog, schemaPattern, typeNamePattern);
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException {
        return WrapperUtils.executeWithPlugins(
                ResultSet.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getSuperTables",
                () -> this.databaseMetaData.getSuperTables(catalog, schemaPattern, tableNamePattern),
                catalog, schemaPattern, tableNamePattern);
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
                                   String attributeNamePattern)
            throws SQLException {
        return WrapperUtils.executeWithPlugins(
                ResultSet.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getAttributes",
                () -> this.databaseMetaData.getAttributes(
                        catalog, schemaPattern, typeNamePattern, attributeNamePattern),
                catalog, schemaPattern, typeNamePattern, attributeNamePattern);
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsResultSetHoldability",
                () -> this.databaseMetaData.supportsResultSetHoldability(holdability),
                holdability);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getResultSetHoldability",
                () -> this.databaseMetaData.getResultSetHoldability());
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getDatabaseMajorVersion",
                () -> this.databaseMetaData.getDatabaseMajorVersion());
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getDatabaseMinorVersion",
                () -> this.databaseMetaData.getDatabaseMinorVersion());
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getJDBCMajorVersion",
                () -> this.databaseMetaData.getJDBCMajorVersion());
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getJDBCMinorVersion",
                () -> this.databaseMetaData.getJDBCMinorVersion());
    }

    @SuppressWarnings("MagicConstant")
    @Override
    public int getSQLStateType() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getSQLStateType",
                () -> this.databaseMetaData.getSQLStateType());
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.locatorsUpdateCopy",
                () -> this.databaseMetaData.locatorsUpdateCopy());
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsStatementPooling",
                () -> this.databaseMetaData.supportsStatementPooling());
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                RowIdLifetime.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getRowIdLifetime",
                () -> this.databaseMetaData.getRowIdLifetime());
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                ResultSet.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getSchemas",
                () -> this.databaseMetaData.getSchemas(catalog, schemaPattern),
                catalog, schemaPattern);
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsStoredFunctionsUsingCallSyntax",
                () -> this.databaseMetaData.supportsStoredFunctionsUsingCallSyntax());
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.autoCommitFailureClosesAllResultSets",
                () -> this.databaseMetaData.autoCommitFailureClosesAllResultSets());
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                ResultSet.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getClientInfoProperties",
                () -> this.databaseMetaData.getClientInfoProperties());
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
            throws SQLException {
        return WrapperUtils.executeWithPlugins(
                ResultSet.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getFunctions",
                () -> this.databaseMetaData.getFunctions(catalog, schemaPattern, functionNamePattern),
                catalog, schemaPattern, functionNamePattern);
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
                                        String columnNamePattern)
            throws SQLException {
        return WrapperUtils.executeWithPlugins(
                ResultSet.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getFunctionColumns",
                () -> this.databaseMetaData.getFunctionColumns(
                        catalog, schemaPattern, functionNamePattern, columnNamePattern),
                catalog, schemaPattern, functionNamePattern, columnNamePattern);
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
                                      String columnNamePattern)
            throws SQLException {
        return WrapperUtils.executeWithPlugins(
                ResultSet.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getPseudoColumns",
                () -> this.databaseMetaData.getPseudoColumns(
                        catalog, schemaPattern, tableNamePattern, columnNamePattern),
                catalog, schemaPattern, tableNamePattern, columnNamePattern);
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.generatedKeyAlwaysReturned",
                () -> this.databaseMetaData.generatedKeyAlwaysReturned());
    }

    @Override
    public long getMaxLogicalLobSize() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                long.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.getMaxLogicalLobSize",
                () -> this.databaseMetaData.getMaxLogicalLobSize());
    }

    @Override
    public boolean supportsRefCursors() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.databaseMetaDataClass,
                "DatabaseMetaData.supportsRefCursors",
                () -> this.databaseMetaData.supportsRefCursors());
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return this.databaseMetaData.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return this.databaseMetaData.isWrapperFor(iface);
    }
}
