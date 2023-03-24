/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package software.amazon.jdbc.wrapper;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.StringJoiner;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.util.DriverInfo;
import software.amazon.jdbc.util.WrapperUtils;

public class DatabaseMetaDataWrapper implements DatabaseMetaData {

  protected DatabaseMetaData databaseMetaData;
  protected ConnectionPluginManager pluginManager;

  public DatabaseMetaDataWrapper(
      @NonNull DatabaseMetaData databaseMetaData, @NonNull ConnectionPluginManager pluginManager) {
    this.databaseMetaData = databaseMetaData;
    this.pluginManager = pluginManager;
  }

  @Override
  public boolean allProceduresAreCallable() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.allProceduresAreCallable",
        () -> this.databaseMetaData.allProceduresAreCallable());
  }

  @Override
  public boolean allTablesAreSelectable() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.allTablesAreSelectable",
        () -> this.databaseMetaData.allTablesAreSelectable());
  }

  @Override
  public String getURL() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        String.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getURL",
        () -> this.databaseMetaData.getURL());
  }

  @Override
  public String getUserName() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        String.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getUserName",
        () -> this.databaseMetaData.getUserName());
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.isReadOnly",
        () -> this.databaseMetaData.isReadOnly());
  }

  @Override
  public boolean nullsAreSortedHigh() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.nullsAreSortedHigh",
        () -> this.databaseMetaData.nullsAreSortedHigh());
  }

  @Override
  public boolean nullsAreSortedLow() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.nullsAreSortedLow",
        () -> this.databaseMetaData.nullsAreSortedLow());
  }

  @Override
  public boolean nullsAreSortedAtStart() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.nullsAreSortedAtStart",
        () -> this.databaseMetaData.nullsAreSortedAtStart());
  }

  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.nullsAreSortedAtEnd",
        () -> this.databaseMetaData.nullsAreSortedAtEnd());
  }

  @Override
  public String getDatabaseProductName() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        String.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getDatabaseProductName",
        () -> this.databaseMetaData.getDatabaseProductName());
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        String.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getDatabaseProductVersion",
        () -> this.databaseMetaData.getDatabaseProductVersion());
  }

  @Override
  public String getDriverName() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        String.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getDriverName",
        () -> DriverInfo.DRIVER_NAME);
  }

  @Override
  public String getDriverVersion() throws SQLException {
    final StringJoiner joiner = new StringJoiner(" ");
    joiner
        .add(DriverInfo.DRIVER_NAME)
        .add(DriverInfo.DRIVER_VERSION)
        .add("( Revision:").add(DriverInfo.REVISION_VERSION).add(")");
    return joiner.toString();
  }

  @Override
  public int getDriverMajorVersion() {
    return DriverInfo.MAJOR_VERSION;
  }

  @Override
  public int getDriverMinorVersion() {
    return DriverInfo.MINOR_VERSION;
  }

  @Override
  public boolean usesLocalFiles() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.usesLocalFiles",
        () -> this.databaseMetaData.usesLocalFiles());
  }

  @Override
  public boolean usesLocalFilePerTable() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.usesLocalFilePerTable",
        () -> this.databaseMetaData.usesLocalFilePerTable());
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsMixedCaseIdentifiers",
        () -> this.databaseMetaData.supportsMixedCaseIdentifiers());
  }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.storesUpperCaseIdentifiers",
        () -> this.databaseMetaData.storesUpperCaseIdentifiers());
  }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.storesLowerCaseIdentifiers",
        () -> this.databaseMetaData.storesLowerCaseIdentifiers());
  }

  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.storesMixedCaseIdentifiers",
        () -> this.databaseMetaData.storesMixedCaseIdentifiers());
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsMixedCaseQuotedIdentifiers",
        () -> this.databaseMetaData.supportsMixedCaseQuotedIdentifiers());
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.storesUpperCaseQuotedIdentifiers",
        () -> this.databaseMetaData.storesUpperCaseQuotedIdentifiers());
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.storesLowerCaseQuotedIdentifiers",
        () -> this.databaseMetaData.storesLowerCaseQuotedIdentifiers());
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.storesMixedCaseQuotedIdentifiers",
        () -> this.databaseMetaData.storesMixedCaseQuotedIdentifiers());
  }

  @Override
  public String getIdentifierQuoteString() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        String.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getIdentifierQuoteString",
        () -> this.databaseMetaData.getIdentifierQuoteString());
  }

  @Override
  public String getSQLKeywords() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        String.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getSQLKeywords",
        () -> this.databaseMetaData.getSQLKeywords());
  }

  @Override
  public String getNumericFunctions() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        String.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getNumericFunctions",
        () -> this.databaseMetaData.getNumericFunctions());
  }

  @Override
  public String getStringFunctions() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        String.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getStringFunctions",
        () -> this.databaseMetaData.getStringFunctions());
  }

  @Override
  public String getSystemFunctions() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        String.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getSystemFunctions",
        () -> this.databaseMetaData.getSystemFunctions());
  }

  @Override
  public String getTimeDateFunctions() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        String.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getTimeDateFunctions",
        () -> this.databaseMetaData.getTimeDateFunctions());
  }

  @Override
  public String getSearchStringEscape() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        String.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getSearchStringEscape",
        () -> this.databaseMetaData.getSearchStringEscape());
  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        String.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getExtraNameCharacters",
        () -> this.databaseMetaData.getExtraNameCharacters());
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsAlterTableWithAddColumn",
        () -> this.databaseMetaData.supportsAlterTableWithAddColumn());
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsAlterTableWithDropColumn",
        () -> this.databaseMetaData.supportsAlterTableWithDropColumn());
  }

  @Override
  public boolean supportsColumnAliasing() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsColumnAliasing",
        () -> this.databaseMetaData.supportsColumnAliasing());
  }

  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.nullPlusNonNullIsNull",
        () -> this.databaseMetaData.nullPlusNonNullIsNull());
  }

  @Override
  public boolean supportsConvert() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsConvert",
        () -> this.databaseMetaData.supportsConvert());
  }

  @Override
  public boolean supportsConvert(int fromType, int toType) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsConvert",
        () -> this.databaseMetaData.supportsConvert(fromType, toType),
        fromType,
        toType);
  }

  @Override
  public boolean supportsTableCorrelationNames() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsTableCorrelationNames",
        () -> this.databaseMetaData.supportsTableCorrelationNames());
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsDifferentTableCorrelationNames",
        () -> this.databaseMetaData.supportsDifferentTableCorrelationNames());
  }

  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsExpressionsInOrderBy",
        () -> this.databaseMetaData.supportsExpressionsInOrderBy());
  }

  @Override
  public boolean supportsOrderByUnrelated() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsOrderByUnrelated",
        () -> this.databaseMetaData.supportsOrderByUnrelated());
  }

  @Override
  public boolean supportsGroupBy() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsGroupBy",
        () -> this.databaseMetaData.supportsGroupBy());
  }

  @Override
  public boolean supportsGroupByUnrelated() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsGroupByUnrelated",
        () -> this.databaseMetaData.supportsGroupByUnrelated());
  }

  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsGroupByBeyondSelect",
        () -> this.databaseMetaData.supportsGroupByBeyondSelect());
  }

  @Override
  public boolean supportsLikeEscapeClause() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsLikeEscapeClause",
        () -> this.databaseMetaData.supportsLikeEscapeClause());
  }

  @Override
  public boolean supportsMultipleResultSets() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsMultipleResultSets",
        () -> this.databaseMetaData.supportsMultipleResultSets());
  }

  @Override
  public boolean supportsMultipleTransactions() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsMultipleTransactions",
        () -> this.databaseMetaData.supportsMultipleTransactions());
  }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsNonNullableColumns",
        () -> this.databaseMetaData.supportsNonNullableColumns());
  }

  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsMinimumSQLGrammar",
        () -> this.databaseMetaData.supportsMinimumSQLGrammar());
  }

  @Override
  public boolean supportsCoreSQLGrammar() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsCoreSQLGrammar",
        () -> this.databaseMetaData.supportsCoreSQLGrammar());
  }

  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsExtendedSQLGrammar",
        () -> this.databaseMetaData.supportsExtendedSQLGrammar());
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsANSI92EntryLevelSQL",
        () -> this.databaseMetaData.supportsANSI92EntryLevelSQL());
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsANSI92IntermediateSQL",
        () -> this.databaseMetaData.supportsANSI92IntermediateSQL());
  }

  @Override
  public boolean supportsANSI92FullSQL() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsANSI92FullSQL",
        () -> this.databaseMetaData.supportsANSI92FullSQL());
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsIntegrityEnhancementFacility",
        () -> this.databaseMetaData.supportsIntegrityEnhancementFacility());
  }

  @Override
  public boolean supportsOuterJoins() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsOuterJoins",
        () -> this.databaseMetaData.supportsOuterJoins());
  }

  @Override
  public boolean supportsFullOuterJoins() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsFullOuterJoins",
        () -> this.databaseMetaData.supportsFullOuterJoins());
  }

  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsLimitedOuterJoins",
        () -> this.databaseMetaData.supportsLimitedOuterJoins());
  }

  @Override
  public String getSchemaTerm() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        String.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getSchemaTerm",
        () -> this.databaseMetaData.getSchemaTerm());
  }

  @Override
  public String getProcedureTerm() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        String.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getProcedureTerm",
        () -> this.databaseMetaData.getProcedureTerm());
  }

  @Override
  public String getCatalogTerm() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        String.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getCatalogTerm",
        () -> this.databaseMetaData.getCatalogTerm());
  }

  @Override
  public boolean isCatalogAtStart() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.isCatalogAtStart",
        () -> this.databaseMetaData.isCatalogAtStart());
  }

  @Override
  public String getCatalogSeparator() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        String.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getCatalogSeparator",
        () -> this.databaseMetaData.getCatalogSeparator());
  }

  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsSchemasInDataManipulation",
        () -> this.databaseMetaData.supportsSchemasInDataManipulation());
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsSchemasInProcedureCalls",
        () -> this.databaseMetaData.supportsSchemasInProcedureCalls());
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsSchemasInTableDefinitions",
        () -> this.databaseMetaData.supportsSchemasInTableDefinitions());
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsSchemasInIndexDefinitions",
        () -> this.databaseMetaData.supportsSchemasInIndexDefinitions());
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsSchemasInPrivilegeDefinitions",
        () -> this.databaseMetaData.supportsSchemasInPrivilegeDefinitions());
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsCatalogsInDataManipulation",
        () -> this.databaseMetaData.supportsCatalogsInDataManipulation());
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsCatalogsInProcedureCalls",
        () -> this.databaseMetaData.supportsCatalogsInProcedureCalls());
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsCatalogsInTableDefinitions",
        () -> this.databaseMetaData.supportsCatalogsInTableDefinitions());
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsCatalogsInIndexDefinitions",
        () -> this.databaseMetaData.supportsCatalogsInIndexDefinitions());
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsCatalogsInPrivilegeDefinitions",
        () -> this.databaseMetaData.supportsCatalogsInPrivilegeDefinitions());
  }

  @Override
  public boolean supportsPositionedDelete() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsPositionedDelete",
        () -> this.databaseMetaData.supportsPositionedDelete());
  }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsPositionedUpdate",
        () -> this.databaseMetaData.supportsPositionedUpdate());
  }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsSelectForUpdate",
        () -> this.databaseMetaData.supportsSelectForUpdate());
  }

  @Override
  public boolean supportsStoredProcedures() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
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
        this.databaseMetaData,
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
        this.databaseMetaData,
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
        this.databaseMetaData,
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
        this.databaseMetaData,
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
        this.databaseMetaData,
        "DatabaseMetaData.supportsCorrelatedSubqueries",
        () -> this.databaseMetaData.supportsCorrelatedSubqueries());
  }

  @Override
  public boolean supportsUnion() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsUnion",
        () -> this.databaseMetaData.supportsUnion());
  }

  @Override
  public boolean supportsUnionAll() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsUnionAll",
        () -> this.databaseMetaData.supportsUnionAll());
  }

  @Override
  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsOpenCursorsAcrossCommit",
        () -> this.databaseMetaData.supportsOpenCursorsAcrossCommit());
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsOpenCursorsAcrossRollback",
        () -> this.databaseMetaData.supportsOpenCursorsAcrossRollback());
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsOpenStatementsAcrossCommit",
        () -> this.databaseMetaData.supportsOpenStatementsAcrossCommit());
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsOpenStatementsAcrossRollback",
        () -> this.databaseMetaData.supportsOpenStatementsAcrossRollback());
  }

  @Override
  public int getMaxBinaryLiteralLength() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getMaxBinaryLiteralLength",
        () -> this.databaseMetaData.getMaxBinaryLiteralLength());
  }

  @Override
  public int getMaxCharLiteralLength() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getMaxCharLiteralLength",
        () -> this.databaseMetaData.getMaxCharLiteralLength());
  }

  @Override
  public int getMaxColumnNameLength() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getMaxColumnNameLength",
        () -> this.databaseMetaData.getMaxColumnNameLength());
  }

  @Override
  public int getMaxColumnsInGroupBy() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getMaxColumnsInGroupBy",
        () -> this.databaseMetaData.getMaxColumnsInGroupBy());
  }

  @Override
  public int getMaxColumnsInIndex() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getMaxColumnsInIndex",
        () -> this.databaseMetaData.getMaxColumnsInIndex());
  }

  @Override
  public int getMaxColumnsInOrderBy() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getMaxColumnsInOrderBy",
        () -> this.databaseMetaData.getMaxColumnsInOrderBy());
  }

  @Override
  public int getMaxColumnsInSelect() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getMaxColumnsInSelect",
        () -> this.databaseMetaData.getMaxColumnsInSelect());
  }

  @Override
  public int getMaxColumnsInTable() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getMaxColumnsInTable",
        () -> this.databaseMetaData.getMaxColumnsInTable());
  }

  @Override
  public int getMaxConnections() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getMaxConnections",
        () -> this.databaseMetaData.getMaxConnections());
  }

  @Override
  public int getMaxCursorNameLength() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getMaxCursorNameLength",
        () -> this.databaseMetaData.getMaxCursorNameLength());
  }

  @Override
  public int getMaxIndexLength() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getMaxIndexLength",
        () -> this.databaseMetaData.getMaxIndexLength());
  }

  @Override
  public int getMaxSchemaNameLength() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getMaxSchemaNameLength",
        () -> this.databaseMetaData.getMaxSchemaNameLength());
  }

  @Override
  public int getMaxProcedureNameLength() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getMaxProcedureNameLength",
        () -> this.databaseMetaData.getMaxProcedureNameLength());
  }

  @Override
  public int getMaxCatalogNameLength() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getMaxCatalogNameLength",
        () -> this.databaseMetaData.getMaxCatalogNameLength());
  }

  @Override
  public int getMaxRowSize() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getMaxRowSize",
        () -> this.databaseMetaData.getMaxRowSize());
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.doesMaxRowSizeIncludeBlobs",
        () -> this.databaseMetaData.doesMaxRowSizeIncludeBlobs());
  }

  @Override
  public int getMaxStatementLength() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getMaxStatementLength",
        () -> this.databaseMetaData.getMaxStatementLength());
  }

  @Override
  public int getMaxStatements() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getMaxStatements",
        () -> this.databaseMetaData.getMaxStatements());
  }

  @Override
  public int getMaxTableNameLength() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getMaxTableNameLength",
        () -> this.databaseMetaData.getMaxTableNameLength());
  }

  @Override
  public int getMaxTablesInSelect() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getMaxTablesInSelect",
        () -> this.databaseMetaData.getMaxTablesInSelect());
  }

  @Override
  public int getMaxUserNameLength() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getMaxUserNameLength",
        () -> this.databaseMetaData.getMaxUserNameLength());
  }

  @Override
  public int getDefaultTransactionIsolation() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getDefaultTransactionIsolation",
        () -> this.databaseMetaData.getDefaultTransactionIsolation());
  }

  @Override
  public boolean supportsTransactions() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsTransactions",
        () -> this.databaseMetaData.supportsTransactions());
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
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
        this.databaseMetaData,
        "DatabaseMetaData.supportsDataDefinitionAndDataManipulationTransactions",
        () -> this.databaseMetaData.supportsDataDefinitionAndDataManipulationTransactions());
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsDataManipulationTransactionsOnly",
        () -> this.databaseMetaData.supportsDataManipulationTransactionsOnly());
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.dataDefinitionCausesTransactionCommit",
        () -> this.databaseMetaData.dataDefinitionCausesTransactionCommit());
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.dataDefinitionIgnoredInTransactions",
        () -> this.databaseMetaData.dataDefinitionIgnoredInTransactions());
  }

  @Override
  public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
      throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getProcedures",
        () -> this.databaseMetaData.getProcedures(catalog, schemaPattern, procedureNamePattern),
        catalog,
        schemaPattern,
        procedureNamePattern);
  }

  @Override
  public ResultSet getProcedureColumns(
      String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern)
      throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getProcedureColumns",
        () ->
            this.databaseMetaData.getProcedureColumns(
                catalog, schemaPattern, procedureNamePattern, columnNamePattern),
        catalog,
        schemaPattern,
        procedureNamePattern,
        columnNamePattern);
  }

  @Override
  public ResultSet getTables(
      String catalog, String schemaPattern, String tableNamePattern, String[] types)
      throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getTables",
        () -> this.databaseMetaData.getTables(catalog, schemaPattern, tableNamePattern, types),
        catalog,
        schemaPattern,
        tableNamePattern,
        types);
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getSchemas",
        () -> this.databaseMetaData.getSchemas());
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getSchemas",
        () -> this.databaseMetaData.getSchemas(catalog, schemaPattern),
        catalog,
        schemaPattern);
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getCatalogs",
        () -> this.databaseMetaData.getCatalogs());
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getTableTypes",
        () -> this.databaseMetaData.getTableTypes());
  }

  @Override
  public ResultSet getColumns(
      String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getColumns",
        () ->
            this.databaseMetaData.getColumns(
                catalog, schemaPattern, tableNamePattern, columnNamePattern),
        catalog,
        schemaPattern,
        tableNamePattern,
        columnNamePattern);
  }

  @Override
  public ResultSet getColumnPrivileges(
      String catalog, String schema, String table, String columnNamePattern) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getColumnPrivileges",
        () -> this.databaseMetaData.getColumnPrivileges(catalog, schema, table, columnNamePattern),
        catalog,
        schema,
        table,
        columnNamePattern);
  }

  @Override
  public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getTablePrivileges",
        () -> this.databaseMetaData.getTablePrivileges(catalog, schemaPattern, tableNamePattern),
        catalog,
        schemaPattern,
        tableNamePattern);
  }

  @Override
  public ResultSet getBestRowIdentifier(
      String catalog, String schema, String table, int scope, boolean nullable)
      throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getBestRowIdentifier",
        () -> this.databaseMetaData.getBestRowIdentifier(catalog, schema, table, scope, nullable),
        catalog,
        schema,
        table,
        scope,
        nullable);
  }

  @Override
  public ResultSet getVersionColumns(String catalog, String schema, String table)
      throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getVersionColumns",
        () -> this.databaseMetaData.getVersionColumns(catalog, schema, table),
        catalog,
        schema,
        table);
  }

  @Override
  public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getPrimaryKeys",
        () -> this.databaseMetaData.getPrimaryKeys(catalog, schema, table),
        catalog,
        schema,
        table);
  }

  @Override
  public ResultSet getImportedKeys(String catalog, String schema, String table)
      throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getImportedKeys",
        () -> this.databaseMetaData.getImportedKeys(catalog, schema, table),
        catalog,
        schema,
        table);
  }

  @Override
  public ResultSet getExportedKeys(String catalog, String schema, String table)
      throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getExportedKeys",
        () -> this.databaseMetaData.getExportedKeys(catalog, schema, table),
        catalog,
        schema,
        table);
  }

  @Override
  public ResultSet getCrossReference(
      String parentCatalog,
      String parentSchema,
      String parentTable,
      String foreignCatalog,
      String foreignSchema,
      String foreignTable)
      throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getCrossReference",
        () ->
            this.databaseMetaData.getCrossReference(
                parentCatalog,
                parentSchema,
                parentTable,
                foreignCatalog,
                foreignSchema,
                foreignTable),
        parentCatalog,
        parentSchema,
        parentTable,
        foreignCatalog,
        foreignSchema,
        foreignTable);
  }

  @Override
  public ResultSet getTypeInfo() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getTypeInfo",
        () -> this.databaseMetaData.getTypeInfo());
  }

  @Override
  public ResultSet getIndexInfo(
      String catalog, String schema, String table, boolean unique, boolean approximate)
      throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getIndexInfo",
        () -> this.databaseMetaData.getIndexInfo(catalog, schema, table, unique, approximate),
        catalog,
        schema,
        table,
        unique,
        approximate);
  }

  @Override
  public boolean supportsResultSetType(int type) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
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
        this.databaseMetaData,
        "DatabaseMetaData.supportsResultSetConcurrency",
        () -> this.databaseMetaData.supportsResultSetConcurrency(type, concurrency),
        type,
        concurrency);
  }

  @Override
  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
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
        this.databaseMetaData,
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
        this.databaseMetaData,
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
        this.databaseMetaData,
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
        this.databaseMetaData,
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
        this.databaseMetaData,
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
        this.databaseMetaData,
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
        this.databaseMetaData,
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
        this.databaseMetaData,
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
        this.databaseMetaData,
        "DatabaseMetaData.supportsBatchUpdates",
        () -> this.databaseMetaData.supportsBatchUpdates());
  }

  @Override
  public ResultSet getUDTs(
      String catalog, String schemaPattern, String typeNamePattern, int[] types)
      throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getUDTs",
        () -> this.databaseMetaData.getUDTs(catalog, schemaPattern, typeNamePattern, types),
        catalog,
        schemaPattern,
        typeNamePattern,
        types);
  }

  @Override
  public Connection getConnection() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Connection.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getConnection",
        () -> this.pluginManager.getConnectionWrapper());
  }

  @SuppressWarnings("SpellCheckingInspection")
  @Override
  public boolean supportsSavepoints() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsSavepoints",
        () -> this.databaseMetaData.supportsSavepoints());
  }

  @Override
  public boolean supportsNamedParameters() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsNamedParameters",
        () -> this.databaseMetaData.supportsNamedParameters());
  }

  @Override
  public boolean supportsMultipleOpenResults() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsMultipleOpenResults",
        () -> this.databaseMetaData.supportsMultipleOpenResults());
  }

  @Override
  public boolean supportsGetGeneratedKeys() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsGetGeneratedKeys",
        () -> this.databaseMetaData.supportsGetGeneratedKeys());
  }

  @Override
  public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
      throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getSuperTypes",
        () -> this.databaseMetaData.getSuperTypes(catalog, schemaPattern, typeNamePattern),
        catalog,
        schemaPattern,
        typeNamePattern);
  }

  @Override
  public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getSuperTables",
        () -> this.databaseMetaData.getSuperTables(catalog, schemaPattern, tableNamePattern),
        catalog,
        schemaPattern,
        tableNamePattern);
  }

  @Override
  public ResultSet getAttributes(
      String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern)
      throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getAttributes",
        () ->
            this.databaseMetaData.getAttributes(
                catalog, schemaPattern, typeNamePattern, attributeNamePattern),
        catalog,
        schemaPattern,
        typeNamePattern,
        attributeNamePattern);
  }

  @Override
  public boolean supportsResultSetHoldability(int holdability) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
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
        this.databaseMetaData,
        "DatabaseMetaData.getResultSetHoldability",
        () -> this.databaseMetaData.getResultSetHoldability());
  }

  @Override
  public int getDatabaseMajorVersion() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getDatabaseMajorVersion",
        () -> this.databaseMetaData.getDatabaseMajorVersion());
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getDatabaseMinorVersion",
        () -> this.databaseMetaData.getDatabaseMinorVersion());
  }

  @Override
  public int getJDBCMajorVersion() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getJDBCMajorVersion",
        () -> this.databaseMetaData.getJDBCMajorVersion());
  }

  @Override
  public int getJDBCMinorVersion() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
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
        this.databaseMetaData,
        "DatabaseMetaData.getSQLStateType",
        () -> this.databaseMetaData.getSQLStateType());
  }

  @Override
  public boolean locatorsUpdateCopy() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.locatorsUpdateCopy",
        () -> this.databaseMetaData.locatorsUpdateCopy());
  }

  @Override
  public boolean supportsStatementPooling() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsStatementPooling",
        () -> this.databaseMetaData.supportsStatementPooling());
  }

  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        RowIdLifetime.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getRowIdLifetime",
        () -> this.databaseMetaData.getRowIdLifetime());
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.supportsStoredFunctionsUsingCallSyntax",
        () -> this.databaseMetaData.supportsStoredFunctionsUsingCallSyntax());
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.autoCommitFailureClosesAllResultSets",
        () -> this.databaseMetaData.autoCommitFailureClosesAllResultSets());
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
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
        this.databaseMetaData,
        "DatabaseMetaData.getFunctions",
        () -> this.databaseMetaData.getFunctions(catalog, schemaPattern, functionNamePattern),
        catalog,
        schemaPattern,
        functionNamePattern);
  }

  @Override
  public ResultSet getFunctionColumns(
      String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern)
      throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getFunctionColumns",
        () ->
            this.databaseMetaData.getFunctionColumns(
                catalog, schemaPattern, functionNamePattern, columnNamePattern),
        catalog,
        schemaPattern,
        functionNamePattern,
        columnNamePattern);
  }

  @Override
  public ResultSet getPseudoColumns(
      String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getPseudoColumns",
        () ->
            this.databaseMetaData.getPseudoColumns(
                catalog, schemaPattern, tableNamePattern, columnNamePattern),
        catalog,
        schemaPattern,
        tableNamePattern,
        columnNamePattern);
  }

  @Override
  public boolean generatedKeyAlwaysReturned() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.generatedKeyAlwaysReturned",
        () -> this.databaseMetaData.generatedKeyAlwaysReturned());
  }

  @Override
  public long getMaxLogicalLobSize() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        long.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        "DatabaseMetaData.getMaxLogicalLobSize",
        () -> this.databaseMetaData.getMaxLogicalLobSize());
  }

  @Override
  public boolean supportsRefCursors() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
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

  @Override
  public String toString() {
    return super.toString() + " - " + this.databaseMetaData;
  }
}
