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
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.util.DriverInfo;
import software.amazon.jdbc.util.WrapperUtils;

public class DatabaseMetaDataWrapper implements DatabaseMetaData {

  protected final DatabaseMetaData databaseMetaData;
  protected final ConnectionWrapper connectionWrapper;
  protected final ConnectionPluginManager pluginManager;

  public DatabaseMetaDataWrapper(
      @NonNull DatabaseMetaData databaseMetaData,
      @NonNull ConnectionWrapper connectionWrapper,
      @NonNull ConnectionPluginManager pluginManager) {
    this.databaseMetaData = databaseMetaData;
    this.connectionWrapper = connectionWrapper;
    this.pluginManager = pluginManager;
  }

  @Override
  public boolean allProceduresAreCallable() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_ALLPROCEDURESARECALLABLE)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_ALLPROCEDURESARECALLABLE,
          this.databaseMetaData::allProceduresAreCallable);
    } else {
      return this.databaseMetaData.allProceduresAreCallable();
    }
  }

  @Override
  public boolean allTablesAreSelectable() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_ALLTABLESARESELECTABLE)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_ALLTABLESARESELECTABLE,
          this.databaseMetaData::allTablesAreSelectable);
    } else {
      return this.databaseMetaData.allTablesAreSelectable();
    }
  }

  @Override
  public String getURL() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETURL)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETURL,
          this.databaseMetaData::getURL);
    } else {
      return this.databaseMetaData.getURL();
    }
  }

  @Override
  public String getUserName() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETUSERNAME)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETUSERNAME,
          this.databaseMetaData::getUserName);
    } else {
      return this.databaseMetaData.getUserName();
    }
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_ISREADONLY)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_ISREADONLY,
          this.databaseMetaData::isReadOnly);
    } else {
      return this.databaseMetaData.isReadOnly();
    }
  }

  @Override
  public boolean nullsAreSortedHigh() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_NULLSARESORTEDHIGH)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_NULLSARESORTEDHIGH,
          this.databaseMetaData::nullsAreSortedHigh);
    } else {
      return this.databaseMetaData.nullsAreSortedHigh();
    }
  }

  @Override
  public boolean nullsAreSortedLow() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_NULLSARESORTEDLOW)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_NULLSARESORTEDLOW,
          this.databaseMetaData::nullsAreSortedLow);
    } else {
      return this.databaseMetaData.nullsAreSortedLow();
    }
  }

  @Override
  public boolean nullsAreSortedAtStart() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_NULLSARESORTEDATSTART)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_NULLSARESORTEDATSTART,
          this.databaseMetaData::nullsAreSortedAtStart);
    } else {
      return this.databaseMetaData.nullsAreSortedAtStart();
    }
  }

  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_NULLSARESORTEDATEND)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_NULLSARESORTEDATEND,
          this.databaseMetaData::nullsAreSortedAtEnd);
    } else {
      return this.databaseMetaData.nullsAreSortedAtEnd();
    }
  }

  @Override
  public String getDatabaseProductName() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETDATABASEPRODUCTNAME)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETDATABASEPRODUCTNAME,
          this.databaseMetaData::getDatabaseProductName);
    } else {
      return this.databaseMetaData.getDatabaseProductName();
    }
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETDATABASEPRODUCTVERSION)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETDATABASEPRODUCTVERSION,
          this.databaseMetaData::getDatabaseProductVersion);
    } else {
      return this.databaseMetaData.getDatabaseProductVersion();
    }
  }

  @Override
  public String getDriverName() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETDRIVERNAME)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETDRIVERNAME,
          () -> DriverInfo.DRIVER_NAME);
    } else {
      return DriverInfo.DRIVER_NAME;
    }
  }

  @Override
  public String getDriverVersion() {
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
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_USESLOCALFILES)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_USESLOCALFILES,
          this.databaseMetaData::usesLocalFiles);
    } else {
      return this.databaseMetaData.usesLocalFiles();
    }
  }

  @Override
  public boolean usesLocalFilePerTable() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_USESLOCALFILEPERTABLE)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_USESLOCALFILEPERTABLE,
          this.databaseMetaData::usesLocalFilePerTable);
    } else {
      return this.databaseMetaData.usesLocalFilePerTable();
    }
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSMIXEDCASEIDENTIFIERS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSMIXEDCASEIDENTIFIERS,
          this.databaseMetaData::supportsMixedCaseIdentifiers);
    } else {
      return this.databaseMetaData.supportsMixedCaseIdentifiers();
    }
  }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_STORESUPPERCASEIDENTIFIERS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_STORESUPPERCASEIDENTIFIERS,
          this.databaseMetaData::storesUpperCaseIdentifiers);
    } else {
      return this.databaseMetaData.storesUpperCaseIdentifiers();
    }
  }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_STORESLOWERCASEIDENTIFIERS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_STORESLOWERCASEIDENTIFIERS,
          this.databaseMetaData::storesLowerCaseIdentifiers);
    } else {
      return this.databaseMetaData.storesLowerCaseIdentifiers();
    }
  }

  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_STORESMIXEDCASEIDENTIFIERS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_STORESMIXEDCASEIDENTIFIERS,
          this.databaseMetaData::storesMixedCaseIdentifiers);
    } else {
      return this.databaseMetaData.storesMixedCaseIdentifiers();
    }
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSMIXEDCASEQUOTEDIDENTIFIERS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSMIXEDCASEQUOTEDIDENTIFIERS,
          this.databaseMetaData::supportsMixedCaseQuotedIdentifiers);
    } else {
      return this.databaseMetaData.supportsMixedCaseQuotedIdentifiers();
    }
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_STORESUPPERCASEQUOTEDIDENTIFIERS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_STORESUPPERCASEQUOTEDIDENTIFIERS,
          this.databaseMetaData::storesUpperCaseQuotedIdentifiers);
    } else {
      return this.databaseMetaData.storesUpperCaseQuotedIdentifiers();
    }
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_STORESLOWERCASEQUOTEDIDENTIFIERS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_STORESLOWERCASEQUOTEDIDENTIFIERS,
          this.databaseMetaData::storesLowerCaseQuotedIdentifiers);
    } else {
      return this.databaseMetaData.storesLowerCaseQuotedIdentifiers();
    }
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_STORESMIXEDCASEQUOTEDIDENTIFIERS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_STORESMIXEDCASEQUOTEDIDENTIFIERS,
          this.databaseMetaData::storesMixedCaseQuotedIdentifiers);
    } else {
      return this.databaseMetaData.storesMixedCaseQuotedIdentifiers();
    }
  }

  @Override
  public String getIdentifierQuoteString() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETIDENTIFIERQUOTESTRING)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETIDENTIFIERQUOTESTRING,
          this.databaseMetaData::getIdentifierQuoteString);
    } else {
      return this.databaseMetaData.getIdentifierQuoteString();
    }
  }

  @Override
  public String getSQLKeywords() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETSQLKEYWORDS)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETSQLKEYWORDS,
          this.databaseMetaData::getSQLKeywords);
    } else {
      return this.databaseMetaData.getSQLKeywords();
    }
  }

  @Override
  public String getNumericFunctions() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETNUMERICFUNCTIONS)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETNUMERICFUNCTIONS,
          this.databaseMetaData::getNumericFunctions);
    } else {
      return this.databaseMetaData.getNumericFunctions();
    }
  }

  @Override
  public String getStringFunctions() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETSTRINGFUNCTIONS)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETSTRINGFUNCTIONS,
          this.databaseMetaData::getStringFunctions);
    } else {
      return this.databaseMetaData.getStringFunctions();
    }
  }

  @Override
  public String getSystemFunctions() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETSYSTEMFUNCTIONS)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETSYSTEMFUNCTIONS,
          this.databaseMetaData::getSystemFunctions);
    } else {
      return this.databaseMetaData.getSystemFunctions();
    }
  }

  @Override
  public String getTimeDateFunctions() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETTIMEDATEFUNCTIONS)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETTIMEDATEFUNCTIONS,
          this.databaseMetaData::getTimeDateFunctions);
    } else {
      return this.databaseMetaData.getTimeDateFunctions();
    }
  }

  @Override
  public String getSearchStringEscape() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETSEARCHSTRINGESCAPE)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETSEARCHSTRINGESCAPE,
          this.databaseMetaData::getSearchStringEscape);
    } else {
      return this.databaseMetaData.getSearchStringEscape();
    }
  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETEXTRANAMECHARACTERS)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETEXTRANAMECHARACTERS,
          this.databaseMetaData::getExtraNameCharacters);
    } else {
      return this.databaseMetaData.getExtraNameCharacters();
    }
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSALTERTABLEWITHADDCOLUMN)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSALTERTABLEWITHADDCOLUMN,
          this.databaseMetaData::supportsAlterTableWithAddColumn);
    } else {
      return this.databaseMetaData.supportsAlterTableWithAddColumn();
    }
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSALTERTABLEWITHDROPCOLUMN)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSALTERTABLEWITHDROPCOLUMN,
          this.databaseMetaData::supportsAlterTableWithDropColumn);
    } else {
      return this.databaseMetaData.supportsAlterTableWithDropColumn();
    }
  }

  @Override
  public boolean supportsColumnAliasing() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSCOLUMNALIASING)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSCOLUMNALIASING,
          this.databaseMetaData::supportsColumnAliasing);
    } else {
      return this.databaseMetaData.supportsColumnAliasing();
    }
  }

  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_NULLPLUSNONNULLISNULL)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_NULLPLUSNONNULLISNULL,
          this.databaseMetaData::nullPlusNonNullIsNull);
    } else {
      return this.databaseMetaData.nullPlusNonNullIsNull();
    }
  }

  @Override
  public boolean supportsConvert() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSCONVERT)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSCONVERT,
          this.databaseMetaData::supportsConvert);
    } else {
      return this.databaseMetaData.supportsConvert();
    }
  }

  @Override
  public boolean supportsConvert(int fromType, int toType) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSCONVERT)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSCONVERT,
          () -> this.databaseMetaData.supportsConvert(fromType, toType),
          fromType,
          toType);
    } else {
      return this.databaseMetaData.supportsConvert(fromType, toType);
    }
  }

  @Override
  public boolean supportsTableCorrelationNames() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSTABLECORRELATIONNAMES)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSTABLECORRELATIONNAMES,
          this.databaseMetaData::supportsTableCorrelationNames);
    } else {
      return this.databaseMetaData.supportsTableCorrelationNames();
    }
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSDIFFERENTTABLECORRELATIONNAMES)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSDIFFERENTTABLECORRELATIONNAMES,
          this.databaseMetaData::supportsDifferentTableCorrelationNames);
    } else {
      return this.databaseMetaData.supportsDifferentTableCorrelationNames();
    }
  }

  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSEXPRESSIONSINORDERBY)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSEXPRESSIONSINORDERBY,
          this.databaseMetaData::supportsExpressionsInOrderBy);
    } else {
      return this.databaseMetaData.supportsExpressionsInOrderBy();
    }
  }

  @Override
  public boolean supportsOrderByUnrelated() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSORDERBYUNRELATED)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSORDERBYUNRELATED,
          this.databaseMetaData::supportsOrderByUnrelated);
    } else {
      return this.databaseMetaData.supportsOrderByUnrelated();
    }
  }

  @Override
  public boolean supportsGroupBy() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSGROUPBY)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSGROUPBY,
          this.databaseMetaData::supportsGroupBy);
    } else {
      return this.databaseMetaData.supportsGroupBy();
    }
  }

  @Override
  public boolean supportsGroupByUnrelated() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSGROUPBYUNRELATED)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSGROUPBYUNRELATED,
          this.databaseMetaData::supportsGroupByUnrelated);
    } else {
      return this.databaseMetaData.supportsGroupByUnrelated();
    }
  }

  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSGROUPBYBEYONDSELECT)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSGROUPBYBEYONDSELECT,
          this.databaseMetaData::supportsGroupByBeyondSelect);
    } else {
      return this.databaseMetaData.supportsGroupByBeyondSelect();
    }
  }

  @Override
  public boolean supportsLikeEscapeClause() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSLIKEESCAPECLAUSE)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSLIKEESCAPECLAUSE,
          this.databaseMetaData::supportsLikeEscapeClause);
    } else {
      return this.databaseMetaData.supportsLikeEscapeClause();
    }
  }

  @Override
  public boolean supportsMultipleResultSets() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSMULTIPLERESULTSETS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSMULTIPLERESULTSETS,
          this.databaseMetaData::supportsMultipleResultSets);
    } else {
      return this.databaseMetaData.supportsMultipleResultSets();
    }
  }

  @Override
  public boolean supportsMultipleTransactions() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSMULTIPLETRANSACTIONS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSMULTIPLETRANSACTIONS,
          this.databaseMetaData::supportsMultipleTransactions);
    } else {
      return this.databaseMetaData.supportsMultipleTransactions();
    }
  }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSNONNULLABLECOLUMNS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSNONNULLABLECOLUMNS,
          this.databaseMetaData::supportsNonNullableColumns);
    } else {
      return this.databaseMetaData.supportsNonNullableColumns();
    }
  }

  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSMINIMUMSQLGRAMMAR)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSMINIMUMSQLGRAMMAR,
          this.databaseMetaData::supportsMinimumSQLGrammar);
    } else {
      return this.databaseMetaData.supportsMinimumSQLGrammar();
    }
  }

  @Override
  public boolean supportsCoreSQLGrammar() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSCORESQLGRAMMAR)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSCORESQLGRAMMAR,
          this.databaseMetaData::supportsCoreSQLGrammar);
    } else {
      return this.databaseMetaData.supportsCoreSQLGrammar();
    }
  }

  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSEXTENDEDSQLGRAMMAR)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSEXTENDEDSQLGRAMMAR,
          this.databaseMetaData::supportsExtendedSQLGrammar);
    } else {
      return this.databaseMetaData.supportsExtendedSQLGrammar();
    }
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSANSI92ENTRYLEVELSQL)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSANSI92ENTRYLEVELSQL,
          this.databaseMetaData::supportsANSI92EntryLevelSQL);
    } else {
      return this.databaseMetaData.supportsANSI92EntryLevelSQL();
    }
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSANSI92INTERMEDIATESQL)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSANSI92INTERMEDIATESQL,
          this.databaseMetaData::supportsANSI92IntermediateSQL);
    } else {
      return this.databaseMetaData.supportsANSI92IntermediateSQL();
    }
  }

  @Override
  public boolean supportsANSI92FullSQL() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSANSI92FULLSQL)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSANSI92FULLSQL,
          this.databaseMetaData::supportsANSI92FullSQL);
    } else {
      return this.databaseMetaData.supportsANSI92FullSQL();
    }
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSINTEGRITYENHANCEMENTFACILITY)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSINTEGRITYENHANCEMENTFACILITY,
          this.databaseMetaData::supportsIntegrityEnhancementFacility);
    } else {
      return this.databaseMetaData.supportsIntegrityEnhancementFacility();
    }
  }

  @Override
  public boolean supportsOuterJoins() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSOUTERJOINS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSOUTERJOINS,
          this.databaseMetaData::supportsOuterJoins);
    } else {
      return this.databaseMetaData.supportsOuterJoins();
    }
  }

  @Override
  public boolean supportsFullOuterJoins() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSFULLOUTERJOINS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSFULLOUTERJOINS,
          this.databaseMetaData::supportsFullOuterJoins);
    } else {
      return this.databaseMetaData.supportsFullOuterJoins();
    }
  }

  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSLIMITEDOUTERJOINS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSLIMITEDOUTERJOINS,
          this.databaseMetaData::supportsLimitedOuterJoins);
    } else {
      return this.databaseMetaData.supportsLimitedOuterJoins();
    }
  }

  @Override
  public String getSchemaTerm() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETSCHEMATERM)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETSCHEMATERM,
          this.databaseMetaData::getSchemaTerm);
    } else {
      return this.databaseMetaData.getSchemaTerm();
    }
  }

  @Override
  public String getProcedureTerm() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETPROCEDURETERM)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETPROCEDURETERM,
          this.databaseMetaData::getProcedureTerm);
    } else {
      return this.databaseMetaData.getProcedureTerm();
    }
  }

  @Override
  public String getCatalogTerm() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETCATALOGTERM)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETCATALOGTERM,
          this.databaseMetaData::getCatalogTerm);
    } else {
      return this.databaseMetaData.getCatalogTerm();
    }
  }

  @Override
  public boolean isCatalogAtStart() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_ISCATALOGATSTART)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_ISCATALOGATSTART,
          this.databaseMetaData::isCatalogAtStart);
    } else {
      return this.databaseMetaData.isCatalogAtStart();
    }
  }

  @Override
  public String getCatalogSeparator() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETCATALOGSEPARATOR)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETCATALOGSEPARATOR,
          this.databaseMetaData::getCatalogSeparator);
    } else {
      return this.databaseMetaData.getCatalogSeparator();
    }
  }

  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSSCHEMASINDATAMANIPULATION)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSSCHEMASINDATAMANIPULATION,
          this.databaseMetaData::supportsSchemasInDataManipulation);
    } else {
      return this.databaseMetaData.supportsSchemasInDataManipulation();
    }
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSSCHEMASINPROCEDURECALLS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSSCHEMASINPROCEDURECALLS,
          this.databaseMetaData::supportsSchemasInProcedureCalls);
    } else {
      return this.databaseMetaData.supportsSchemasInProcedureCalls();
    }
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSSCHEMASINTABLEDEFINITIONS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSSCHEMASINTABLEDEFINITIONS,
          this.databaseMetaData::supportsSchemasInTableDefinitions);
    } else {
      return this.databaseMetaData.supportsSchemasInTableDefinitions();
    }
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSSCHEMASININDEXDEFINITIONS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSSCHEMASININDEXDEFINITIONS,
          this.databaseMetaData::supportsSchemasInIndexDefinitions);
    } else {
      return this.databaseMetaData.supportsSchemasInIndexDefinitions();
    }
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSSCHEMASINPRIVILEGEDEFINITIONS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSSCHEMASINPRIVILEGEDEFINITIONS,
          this.databaseMetaData::supportsSchemasInPrivilegeDefinitions);
    } else {
      return this.databaseMetaData.supportsSchemasInPrivilegeDefinitions();
    }
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSCATALOGSINDATAMANIPULATION)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSCATALOGSINDATAMANIPULATION,
          this.databaseMetaData::supportsCatalogsInDataManipulation);
    } else {
      return this.databaseMetaData.supportsCatalogsInDataManipulation();
    }
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSCATALOGSINPROCEDURECALLS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSCATALOGSINPROCEDURECALLS,
          this.databaseMetaData::supportsCatalogsInProcedureCalls);
    } else {
      return this.databaseMetaData.supportsCatalogsInProcedureCalls();
    }
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSCATALOGSINTABLEDEFINITIONS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSCATALOGSINTABLEDEFINITIONS,
          this.databaseMetaData::supportsCatalogsInTableDefinitions);
    } else {
      return this.databaseMetaData.supportsCatalogsInTableDefinitions();
    }
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSCATALOGSININDEXDEFINITIONS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSCATALOGSININDEXDEFINITIONS,
          this.databaseMetaData::supportsCatalogsInIndexDefinitions);
    } else {
      return this.databaseMetaData.supportsCatalogsInIndexDefinitions();
    }
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSCATALOGSINPRIVILEGEDEFINITIONS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSCATALOGSINPRIVILEGEDEFINITIONS,
          this.databaseMetaData::supportsCatalogsInPrivilegeDefinitions);
    } else {
      return this.databaseMetaData.supportsCatalogsInPrivilegeDefinitions();
    }
  }

  @Override
  public boolean supportsPositionedDelete() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSPOSITIONEDDELETE)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSPOSITIONEDDELETE,
          this.databaseMetaData::supportsPositionedDelete);
    } else {
      return this.databaseMetaData.supportsPositionedDelete();
    }
  }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSPOSITIONEDUPDATE)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSPOSITIONEDUPDATE,
          this.databaseMetaData::supportsPositionedUpdate);
    } else {
      return this.databaseMetaData.supportsPositionedUpdate();
    }
  }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSSELECTFORUPDATE)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSSELECTFORUPDATE,
          this.databaseMetaData::supportsSelectForUpdate);
    } else {
      return this.databaseMetaData.supportsSelectForUpdate();
    }
  }

  @Override
  public boolean supportsStoredProcedures() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSSTOREDPROCEDURES)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSSTOREDPROCEDURES,
          this.databaseMetaData::supportsStoredProcedures);
    } else {
      return this.databaseMetaData.supportsStoredProcedures();
    }
  }

  @Override
  public boolean supportsSubqueriesInComparisons() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSSUBQUERIESINCOMPARISONS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSSUBQUERIESINCOMPARISONS,
          this.databaseMetaData::supportsSubqueriesInComparisons);
    } else {
      return this.databaseMetaData.supportsSubqueriesInComparisons();
    }
  }

  @Override
  public boolean supportsSubqueriesInExists() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSSUBQUERIESINEXISTS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSSUBQUERIESINEXISTS,
          this.databaseMetaData::supportsSubqueriesInExists);
    } else {
      return this.databaseMetaData.supportsSubqueriesInExists();
    }
  }

  @Override
  public boolean supportsSubqueriesInIns() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSSUBQUERIESININS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSSUBQUERIESININS,
          this.databaseMetaData::supportsSubqueriesInIns);
    } else {
      return this.databaseMetaData.supportsSubqueriesInIns();
    }
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSSUBQUERIESINQUANTIFIEDS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSSUBQUERIESINQUANTIFIEDS,
          this.databaseMetaData::supportsSubqueriesInQuantifieds);
    } else {
      return this.databaseMetaData.supportsSubqueriesInQuantifieds();
    }
  }

  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSCORRELATEDSUBQUERIES)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSCORRELATEDSUBQUERIES,
          this.databaseMetaData::supportsCorrelatedSubqueries);
    } else {
      return this.databaseMetaData.supportsCorrelatedSubqueries();
    }
  }

  @Override
  public boolean supportsUnion() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSUNION)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSUNION,
          this.databaseMetaData::supportsUnion);
    } else {
      return this.databaseMetaData.supportsUnion();
    }
  }

  @Override
  public boolean supportsUnionAll() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSUNIONALL)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSUNIONALL,
          this.databaseMetaData::supportsUnionAll);
    } else {
      return this.databaseMetaData.supportsUnionAll();
    }
  }

  @Override
  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSOPENCURSORSACROSSCOMMIT)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSOPENCURSORSACROSSCOMMIT,
          this.databaseMetaData::supportsOpenCursorsAcrossCommit);
    } else {
      return this.databaseMetaData.supportsOpenCursorsAcrossCommit();
    }
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSOPENCURSORSACROSSROLLBACK)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSOPENCURSORSACROSSROLLBACK,
          this.databaseMetaData::supportsOpenCursorsAcrossRollback);
    } else {
      return this.databaseMetaData.supportsOpenCursorsAcrossRollback();
    }
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSOPENSTATEMENTSACROSSCOMMIT)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSOPENSTATEMENTSACROSSCOMMIT,
          this.databaseMetaData::supportsOpenStatementsAcrossCommit);
    } else {
      return this.databaseMetaData.supportsOpenStatementsAcrossCommit();
    }
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSOPENSTATEMENTSACROSSROLLBACK)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSOPENSTATEMENTSACROSSROLLBACK,
          this.databaseMetaData::supportsOpenStatementsAcrossRollback);
    } else {
      return this.databaseMetaData.supportsOpenStatementsAcrossRollback();
    }
  }

  @Override
  public int getMaxBinaryLiteralLength() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETMAXBINARYLITERALLENGTH)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETMAXBINARYLITERALLENGTH,
          this.databaseMetaData::getMaxBinaryLiteralLength);
    } else {
      return this.databaseMetaData.getMaxBinaryLiteralLength();
    }
  }

  @Override
  public int getMaxCharLiteralLength() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETMAXCHARLITERALLENGTH)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETMAXCHARLITERALLENGTH,
          this.databaseMetaData::getMaxCharLiteralLength);
    } else {
      return this.databaseMetaData.getMaxCharLiteralLength();
    }
  }

  @Override
  public int getMaxColumnNameLength() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETMAXCOLUMNNAMELENGTH)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETMAXCOLUMNNAMELENGTH,
          this.databaseMetaData::getMaxColumnNameLength);
    } else {
      return this.databaseMetaData.getMaxColumnNameLength();
    }
  }

  @Override
  public int getMaxColumnsInGroupBy() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETMAXCOLUMNSINGROUPBY)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETMAXCOLUMNSINGROUPBY,
          this.databaseMetaData::getMaxColumnsInGroupBy);
    } else {
      return this.databaseMetaData.getMaxColumnsInGroupBy();
    }
  }

  @Override
  public int getMaxColumnsInIndex() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETMAXCOLUMNSININDEX)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETMAXCOLUMNSININDEX,
          this.databaseMetaData::getMaxColumnsInIndex);
    } else {
      return this.databaseMetaData.getMaxColumnsInIndex();
    }
  }

  @Override
  public int getMaxColumnsInOrderBy() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETMAXCOLUMNSINORDERBY)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETMAXCOLUMNSINORDERBY,
          this.databaseMetaData::getMaxColumnsInOrderBy);
    } else {
      return this.databaseMetaData.getMaxColumnsInOrderBy();
    }
  }

  @Override
  public int getMaxColumnsInSelect() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETMAXCOLUMNSINSELECT)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETMAXCOLUMNSINSELECT,
          this.databaseMetaData::getMaxColumnsInSelect);
    } else {
      return this.databaseMetaData.getMaxColumnsInSelect();
    }
  }

  @Override
  public int getMaxColumnsInTable() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETMAXCOLUMNSINTABLE)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETMAXCOLUMNSINTABLE,
          this.databaseMetaData::getMaxColumnsInTable);
    } else {
      return this.databaseMetaData.getMaxColumnsInTable();
    }
  }

  @Override
  public int getMaxConnections() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETMAXCONNECTIONS)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETMAXCONNECTIONS,
          this.databaseMetaData::getMaxConnections);
    } else {
      return this.databaseMetaData.getMaxConnections();
    }
  }

  @Override
  public int getMaxCursorNameLength() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETMAXCURSORNAMELENGTH)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETMAXCURSORNAMELENGTH,
          this.databaseMetaData::getMaxCursorNameLength);
    } else {
      return this.databaseMetaData.getMaxCursorNameLength();
    }
  }

  @Override
  public int getMaxIndexLength() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETMAXINDEXLENGTH)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETMAXINDEXLENGTH,
          this.databaseMetaData::getMaxIndexLength);
    } else {
      return this.databaseMetaData.getMaxIndexLength();
    }
  }

  @Override
  public int getMaxSchemaNameLength() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETMAXSCHEMANAMELENGTH)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETMAXSCHEMANAMELENGTH,
          this.databaseMetaData::getMaxSchemaNameLength);
    } else {
      return this.databaseMetaData.getMaxSchemaNameLength();
    }
  }

  @Override
  public int getMaxProcedureNameLength() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETMAXPROCEDURENAMELENGTH)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETMAXPROCEDURENAMELENGTH,
          this.databaseMetaData::getMaxProcedureNameLength);
    } else {
      return this.databaseMetaData.getMaxProcedureNameLength();
    }
  }

  @Override
  public int getMaxCatalogNameLength() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETMAXCATALOGNAMELENGTH)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETMAXCATALOGNAMELENGTH,
          this.databaseMetaData::getMaxCatalogNameLength);
    } else {
      return this.databaseMetaData.getMaxCatalogNameLength();
    }
  }

  @Override
  public int getMaxRowSize() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETMAXROWSIZE)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETMAXROWSIZE,
          this.databaseMetaData::getMaxRowSize);
    } else {
      return this.databaseMetaData.getMaxRowSize();
    }
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_DOESMAXROWSIZEINCLUDEBLOBS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_DOESMAXROWSIZEINCLUDEBLOBS,
          this.databaseMetaData::doesMaxRowSizeIncludeBlobs);
    } else {
      return this.databaseMetaData.doesMaxRowSizeIncludeBlobs();
    }
  }

  @Override
  public int getMaxStatementLength() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETMAXSTATEMENTLENGTH)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETMAXSTATEMENTLENGTH,
          this.databaseMetaData::getMaxStatementLength);
    } else {
      return this.databaseMetaData.getMaxStatementLength();
    }
  }

  @Override
  public int getMaxStatements() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETMAXSTATEMENTS)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETMAXSTATEMENTS,
          this.databaseMetaData::getMaxStatements);
    } else {
      return this.databaseMetaData.getMaxStatements();
    }
  }

  @Override
  public int getMaxTableNameLength() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETMAXTABLENAMELENGTH)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETMAXTABLENAMELENGTH,
          this.databaseMetaData::getMaxTableNameLength);
    } else {
      return this.databaseMetaData.getMaxTableNameLength();
    }
  }

  @Override
  public int getMaxTablesInSelect() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETMAXTABLESINSELECT)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETMAXTABLESINSELECT,
          this.databaseMetaData::getMaxTablesInSelect);
    } else {
      return this.databaseMetaData.getMaxTablesInSelect();
    }
  }

  @Override
  public int getMaxUserNameLength() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETMAXUSERNAMELENGTH)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETMAXUSERNAMELENGTH,
          this.databaseMetaData::getMaxUserNameLength);
    } else {
      return this.databaseMetaData.getMaxUserNameLength();
    }
  }

  @Override
  public int getDefaultTransactionIsolation() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETDEFAULTTRANSACTIONISOLATION)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETDEFAULTTRANSACTIONISOLATION,
          this.databaseMetaData::getDefaultTransactionIsolation);
    } else {
      return this.databaseMetaData.getDefaultTransactionIsolation();
    }
  }

  @Override
  public boolean supportsTransactions() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSTRANSACTIONS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSTRANSACTIONS,
          this.databaseMetaData::supportsTransactions);
    } else {
      return this.databaseMetaData.supportsTransactions();
    }
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSTRANSACTIONISOLATIONLEVEL)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSTRANSACTIONISOLATIONLEVEL,
          () -> this.databaseMetaData.supportsTransactionIsolationLevel(level),
          level);
    } else {
      return this.databaseMetaData.supportsTransactionIsolationLevel(level);
    }
  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
    if (this.pluginManager.mustUsePipeline(
        JdbcMethod.DATABASEMETADATA_SUPPORTSDATADEFINITIONANDDATAMANIPULATIONTRANSACTIONS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSDATADEFINITIONANDDATAMANIPULATIONTRANSACTIONS,
          this.databaseMetaData::supportsDataDefinitionAndDataManipulationTransactions);
    } else {
      return this.databaseMetaData.supportsDataDefinitionAndDataManipulationTransactions();
    }
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSDATAMANIPULATIONTRANSACTIONSONLY)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSDATAMANIPULATIONTRANSACTIONSONLY,
          this.databaseMetaData::supportsDataManipulationTransactionsOnly);
    } else {
      return this.databaseMetaData.supportsDataManipulationTransactionsOnly();
    }
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_DATADEFINITIONCAUSESTRANSACTIONCOMMIT)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_DATADEFINITIONCAUSESTRANSACTIONCOMMIT,
          this.databaseMetaData::dataDefinitionCausesTransactionCommit);
    } else {
      return this.databaseMetaData.dataDefinitionCausesTransactionCommit();
    }
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_DATADEFINITIONIGNOREDINTRANSACTIONS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_DATADEFINITIONIGNOREDINTRANSACTIONS,
          this.databaseMetaData::dataDefinitionIgnoredInTransactions);
    } else {
      return this.databaseMetaData.dataDefinitionIgnoredInTransactions();
    }
  }

  @Override
  public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
      throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        JdbcMethod.DATABASEMETADATA_GETPROCEDURES,
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
        JdbcMethod.DATABASEMETADATA_GETPROCEDURECOLUMNS,
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
        JdbcMethod.DATABASEMETADATA_GETTABLES,
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
        JdbcMethod.DATABASEMETADATA_GETSCHEMAS,
        this.databaseMetaData::getSchemas);
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        JdbcMethod.DATABASEMETADATA_GETSCHEMAS,
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
        JdbcMethod.DATABASEMETADATA_GETCATALOGS,
        this.databaseMetaData::getCatalogs);
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        JdbcMethod.DATABASEMETADATA_GETTABLETYPES,
        this.databaseMetaData::getTableTypes);
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
        JdbcMethod.DATABASEMETADATA_GETCOLUMNS,
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
        JdbcMethod.DATABASEMETADATA_GETCOLUMNPRIVILEGES,
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
        JdbcMethod.DATABASEMETADATA_GETTABLEPRIVILEGES,
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
        JdbcMethod.DATABASEMETADATA_GETBESTROWIDENTIFIER,
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
        JdbcMethod.DATABASEMETADATA_GETVERSIONCOLUMNS,
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
        JdbcMethod.DATABASEMETADATA_GETPRIMARYKEYS,
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
        JdbcMethod.DATABASEMETADATA_GETIMPORTEDKEYS,
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
        JdbcMethod.DATABASEMETADATA_GETEXPORTEDKEYS,
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
        JdbcMethod.DATABASEMETADATA_GETCROSSREFERENCE,
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
        JdbcMethod.DATABASEMETADATA_GETTYPEINFO,
        this.databaseMetaData::getTypeInfo);
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
        JdbcMethod.DATABASEMETADATA_GETINDEXINFO,
        () -> this.databaseMetaData.getIndexInfo(catalog, schema, table, unique, approximate),
        catalog,
        schema,
        table,
        unique,
        approximate);
  }

  @Override
  public boolean supportsResultSetType(int type) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSRESULTSETTYPE)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSRESULTSETTYPE,
          () -> this.databaseMetaData.supportsResultSetType(type),
          type);
    } else {
      return this.databaseMetaData.supportsResultSetType(type);
    }
  }

  @Override
  public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSRESULTSETCONCURRENCY)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSRESULTSETCONCURRENCY,
          () -> this.databaseMetaData.supportsResultSetConcurrency(type, concurrency),
          type,
          concurrency);
    } else {
      return this.databaseMetaData.supportsResultSetConcurrency(type, concurrency);
    }
  }

  @Override
  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_OWNUPDATESAREVISIBLE)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_OWNUPDATESAREVISIBLE,
          () -> this.databaseMetaData.ownUpdatesAreVisible(type),
          type);
    } else {
      return this.databaseMetaData.ownUpdatesAreVisible(type);
    }
  }

  @Override
  public boolean ownDeletesAreVisible(int type) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_OWNDELETESAREVISIBLE)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_OWNDELETESAREVISIBLE,
          () -> this.databaseMetaData.ownDeletesAreVisible(type),
          type);
    } else {
      return this.databaseMetaData.ownDeletesAreVisible(type);
    }
  }

  @Override
  public boolean ownInsertsAreVisible(int type) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_OWNINSERTSAREVISIBLE)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_OWNINSERTSAREVISIBLE,
          () -> this.databaseMetaData.ownInsertsAreVisible(type),
          type);
    } else {
      return this.databaseMetaData.ownInsertsAreVisible(type);
    }
  }

  @Override
  public boolean othersUpdatesAreVisible(int type) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_OTHERSUPDATESAREVISIBLE)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_OTHERSUPDATESAREVISIBLE,
          () -> this.databaseMetaData.othersUpdatesAreVisible(type),
          type);
    } else {
      return this.databaseMetaData.othersUpdatesAreVisible(type);
    }
  }

  @Override
  public boolean othersDeletesAreVisible(int type) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_OTHERSDELETESAREVISIBLE)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_OTHERSDELETESAREVISIBLE,
          () -> this.databaseMetaData.othersDeletesAreVisible(type),
          type);
    } else {
      return this.databaseMetaData.othersDeletesAreVisible(type);
    }
  }

  @Override
  public boolean othersInsertsAreVisible(int type) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_OTHERSINSERTSAREVISIBLE)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_OTHERSINSERTSAREVISIBLE,
          () -> this.databaseMetaData.othersInsertsAreVisible(type),
          type);
    } else {
      return this.databaseMetaData.othersInsertsAreVisible(type);
    }
  }

  @Override
  public boolean updatesAreDetected(int type) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_UPDATESAREDETECTED)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_UPDATESAREDETECTED,
          () -> this.databaseMetaData.updatesAreDetected(type),
          type);
    } else {
      return this.databaseMetaData.updatesAreDetected(type);
    }
  }

  @Override
  public boolean deletesAreDetected(int type) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_DELETESAREDETECTED)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_DELETESAREDETECTED,
          () -> this.databaseMetaData.deletesAreDetected(type),
          type);
    } else {
      return this.databaseMetaData.deletesAreDetected(type);
    }
  }

  @Override
  public boolean insertsAreDetected(int type) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_INSERTSAREDETECTED)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_INSERTSAREDETECTED,
          () -> this.databaseMetaData.insertsAreDetected(type),
          type);
    } else {
      return this.databaseMetaData.insertsAreDetected(type);
    }
  }

  @Override
  public boolean supportsBatchUpdates() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSBATCHUPDATES)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSBATCHUPDATES,
          this.databaseMetaData::supportsBatchUpdates);
    } else {
      return this.databaseMetaData.supportsBatchUpdates();
    }
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
        JdbcMethod.DATABASEMETADATA_GETUDTS,
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
        JdbcMethod.DATABASEMETADATA_GETCONNECTION,
        () -> this.connectionWrapper);
  }

  @Override
  public boolean supportsSavepoints() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSSAVEPOINTS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSSAVEPOINTS,
          this.databaseMetaData::supportsSavepoints);
    } else {
      return this.databaseMetaData.supportsSavepoints();
    }
  }

  @Override
  public boolean supportsNamedParameters() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSNAMEDPARAMETERS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSNAMEDPARAMETERS,
          this.databaseMetaData::supportsNamedParameters);
    } else {
      return this.databaseMetaData.supportsNamedParameters();
    }
  }

  @Override
  public boolean supportsMultipleOpenResults() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSMULTIPLEOPENRESULTS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSMULTIPLEOPENRESULTS,
          this.databaseMetaData::supportsMultipleOpenResults);
    } else {
      return this.databaseMetaData.supportsMultipleOpenResults();
    }
  }

  @Override
  public boolean supportsGetGeneratedKeys() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSGETGENERATEDKEYS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSGETGENERATEDKEYS,
          this.databaseMetaData::supportsGetGeneratedKeys);
    } else {
      return this.databaseMetaData.supportsGetGeneratedKeys();
    }
  }

  @Override
  public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
      throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        JdbcMethod.DATABASEMETADATA_GETSUPERTYPES,
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
        JdbcMethod.DATABASEMETADATA_GETSUPERTABLES,
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
        JdbcMethod.DATABASEMETADATA_GETATTRIBUTES,
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
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSRESULTSETHOLDABILITY)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSRESULTSETHOLDABILITY,
          () -> this.databaseMetaData.supportsResultSetHoldability(holdability),
          holdability);
    } else {
      return this.databaseMetaData.supportsResultSetHoldability(holdability);
    }
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETRESULTSETHOLDABILITY)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETRESULTSETHOLDABILITY,
          this.databaseMetaData::getResultSetHoldability);
    } else {
      return this.databaseMetaData.getResultSetHoldability();
    }
  }

  @Override
  public int getDatabaseMajorVersion() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETDATABASEMAJORVERSION)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETDATABASEMAJORVERSION,
          this.databaseMetaData::getDatabaseMajorVersion);
    } else {
      return this.databaseMetaData.getDatabaseMajorVersion();
    }
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETDATABASEMINORVERSION)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETDATABASEMINORVERSION,
          this.databaseMetaData::getDatabaseMinorVersion);
    } else {
      return this.databaseMetaData.getDatabaseMinorVersion();
    }
  }

  @Override
  public int getJDBCMajorVersion() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETJDBCMAJORVERSION)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETJDBCMAJORVERSION,
          this.databaseMetaData::getJDBCMajorVersion);
    } else {
      return this.databaseMetaData.getJDBCMajorVersion();
    }
  }

  @Override
  public int getJDBCMinorVersion() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETJDBCMINORVERSION)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETJDBCMINORVERSION,
          this.databaseMetaData::getJDBCMinorVersion);
    } else {
      return this.databaseMetaData.getJDBCMinorVersion();
    }
  }

  @SuppressWarnings("MagicConstant")
  @Override
  public int getSQLStateType() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETSQLSTATETYPE)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETSQLSTATETYPE,
          this.databaseMetaData::getSQLStateType);
    } else {
      return this.databaseMetaData.getSQLStateType();
    }
  }

  @Override
  public boolean locatorsUpdateCopy() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_LOCATORSUPDATECOPY)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_LOCATORSUPDATECOPY,
          this.databaseMetaData::locatorsUpdateCopy);
    } else {
      return this.databaseMetaData.locatorsUpdateCopy();
    }
  }

  @Override
  public boolean supportsStatementPooling() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSSTATEMENTPOOLING)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSSTATEMENTPOOLING,
          this.databaseMetaData::supportsStatementPooling);
    } else {
      return this.databaseMetaData.supportsStatementPooling();
    }
  }

  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETROWIDLIFETIME)) {
      return WrapperUtils.executeWithPlugins(
          RowIdLifetime.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETROWIDLIFETIME,
          this.databaseMetaData::getRowIdLifetime);
    } else {
      return this.databaseMetaData.getRowIdLifetime();
    }
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSSTOREDFUNCTIONSUSINGCALLSYNTAX)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSSTOREDFUNCTIONSUSINGCALLSYNTAX,
          this.databaseMetaData::supportsStoredFunctionsUsingCallSyntax);
    } else {
      return this.databaseMetaData.supportsStoredFunctionsUsingCallSyntax();
    }
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_AUTOCOMMITFAILURECLOSESALLRESULTSETS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_AUTOCOMMITFAILURECLOSESALLRESULTSETS,
          this.databaseMetaData::autoCommitFailureClosesAllResultSets);
    } else {
      return this.databaseMetaData.autoCommitFailureClosesAllResultSets();
    }
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        JdbcMethod.DATABASEMETADATA_GETCLIENTINFOPROPERTIES,
        this.databaseMetaData::getClientInfoProperties);
  }

  @Override
  public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
      throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.databaseMetaData,
        JdbcMethod.DATABASEMETADATA_GETFUNCTIONS,
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
        JdbcMethod.DATABASEMETADATA_GETFUNCTIONCOLUMNS,
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
        JdbcMethod.DATABASEMETADATA_GETPSEUDOCOLUMNS,
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
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GENERATEDKEYALWAYSRETURNED)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GENERATEDKEYALWAYSRETURNED,
          this.databaseMetaData::generatedKeyAlwaysReturned);
    } else {
      return this.databaseMetaData.generatedKeyAlwaysReturned();
    }
  }

  @Override
  public long getMaxLogicalLobSize() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_GETMAXLOGICALLOBSIZE)) {
      return WrapperUtils.executeWithPlugins(
          long.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_GETMAXLOGICALLOBSIZE,
          this.databaseMetaData::getMaxLogicalLobSize);
    } else {
      return this.databaseMetaData.getMaxLogicalLobSize();
    }
  }

  @Override
  public boolean supportsRefCursors() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.DATABASEMETADATA_SUPPORTSREFCURSORS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.databaseMetaData,
          JdbcMethod.DATABASEMETADATA_SUPPORTSREFCURSORS,
          this.databaseMetaData::supportsRefCursors);
    } else {
      return this.databaseMetaData.supportsRefCursors();
    }
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
