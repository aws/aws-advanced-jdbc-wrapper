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

package software.amazon.jdbc.integration.orchestra;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import integration.DatabaseEngine;
import integration.DatabaseEngineDeployment;
import integration.TestGlobalDatabaseInfo;
import integration.TestRegionalClusterInfo;
import integration.container.ProxyHelper;
import integration.orchestra.EngineAliases;
import integration.orchestra.GradleRuntime;
import integration.orchestra.HibernateGradleRuntime;
import integration.orchestra.OrchestraTestPolicy;
import integration.util.XaTestUtility;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class OrchestraMigrationSupportTest {

  @Test
  void engineAliasesAreConsistent() {
    assertArrayEquals(
        new DatabaseEngine[] {DatabaseEngine.PG, DatabaseEngine.PG, DatabaseEngine.PG,
            DatabaseEngine.MYSQL, DatabaseEngine.MARIADB},
        EngineAliases.requested("pg,postgres,postgresql,mysql,mariadb", DatabaseEngine.PG, true));
    assertThrows(IllegalArgumentException.class,
        () -> EngineAliases.requested("mariadb", DatabaseEngine.PG, false));
  }

  @Test
  void globalDatabaseUsesAuroraLifecycleRules() {
    assertTrue(DatabaseEngineDeployment.AURORA.isAurora());
    assertTrue(DatabaseEngineDeployment.AURORA_GLOBAL.isAurora());
    assertFalse(DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER.isAurora());
  }

  @Test
  void multipleDockerCompositionsContinueAfterFailure() {
    assertFalse(OrchestraTestPolicy.continueAfterFailure(1, 1));
    assertTrue(OrchestraTestPolicy.continueAfterFailure(2, 1));
    assertTrue(OrchestraTestPolicy.continueAfterFailure(1, 2));
  }

  @Test
  void strictImpairmentPropagatesGatewayFailuresWhileRestoreIsBestEffort() {
    final ProxyHelper.GatewayCall failure = () -> {
      throw new IOException();
    };

    assertThrows(UncheckedIOException.class,
        () -> ProxyHelper.strictGateway(failure, "disable destination"));
    ProxyHelper.bestEffortGateway(failure, "restore destination");
    assertThrows(IllegalArgumentException.class,
        () -> ProxyHelper.required(java.util.Collections.emptyList(), "unknown region"));
  }

  @Test
  void cleanupCoversPrimaryAndEverySecondaryRegion() {
    assertEquals(
        Arrays.asList("us-east-2", "us-west-2", "eu-central-1"),
        OrchestraTestPolicy.authorizedRegions(
            "us-east-2", Arrays.asList("us-west-2", "eu-central-1", "us-east-2")));
  }

  @Test
  void missingXaRecoverPrivilegeFailsDockerAndSkipsManagedDeployments() {
    assertFalse(XaTestUtility.skipMissingXaRecoverPrivilege(DatabaseEngineDeployment.DOCKER));
    assertTrue(XaTestUtility.skipMissingXaRecoverPrivilege(DatabaseEngineDeployment.AURORA));
    assertTrue(XaTestUtility.skipMissingXaRecoverPrivilege(DatabaseEngineDeployment.AURORA_GLOBAL));
  }

  @Test
  void shardSelectionAppliesClassAndMethodSuiteTags() {
    assertFalse(ShardSelection.classSelected(
        set("gdb"), Collections.emptyList(), Collections.emptySet(), set("gdb")));
    assertTrue(ShardSelection.classSelected(
        Collections.emptySet(), Arrays.asList(set("caching")), set("caching"), Collections.emptySet()));
    assertFalse(ShardSelection.classSelected(
        Collections.emptySet(), Arrays.asList(set("xa")), set("caching"), Collections.emptySet()));
  }

  @Test
  void globalHostValidationRejectsMissingSuffixAndOrdersPrimaryFirst() {
    final TestRegionalClusterInfo secondary = region("us-west-2", "suffix-west");
    final TestRegionalClusterInfo primary = region("us-east-2", "suffix-east");
    final TestGlobalDatabaseInfo info = new TestGlobalDatabaseInfo();
    info.setPrimaryRegion("us-east-2");
    info.setRegions(Arrays.asList(secondary, primary));

    assertEquals(Arrays.asList("us-east-2", "us-west-2"), info.getRegionNames());
    assertEquals("?.suffix-east,?.suffix-west", info.getInstanceHostPatterns());

    secondary.setInstanceEndpointSuffix(null);
    assertThrows(IllegalStateException.class, info::getInstanceHostPatterns);
  }

  @Test
  void gradleRuntimeUsesBuildSuppliedPaths(@TempDir final Path temporaryDirectory) throws IOException {
    final Path installation = Files.createDirectory(temporaryDirectory.resolve("gradle-installation"));
    final Path cacheArchive = Files.createFile(temporaryDirectory.resolve("gradle-modules.jar"));
    final String previousInstallation = System.getProperty(GradleRuntime.INSTALLATION_PROPERTY);
    final String previousCacheArchive =
        System.getProperty(GradleRuntime.MODULE_CACHE_ARCHIVE_PROPERTY);

    try {
      System.setProperty(GradleRuntime.INSTALLATION_PROPERTY, installation.toString());
      System.setProperty(GradleRuntime.MODULE_CACHE_ARCHIVE_PROPERTY, cacheArchive.toString());

      final Map<Path, String> binds = GradleRuntime.containerBinds();
      final Map<Path, String> copies = GradleRuntime.containerCopies();
      assertEquals("/opt/orchestra-gradle", binds.get(installation.toAbsolutePath()));
      assertEquals("/opt/orchestra-gradle-modules.jar", copies.get(cacheArchive.toAbsolutePath()));
      assertEquals("/opt/orchestra-gradle-launcher", GradleRuntime.containerCommand());
      assertEquals(
          "/opt/orchestra-gradle-user-home",
          GradleRuntime.containerEnvironment().get("GRADLE_USER_HOME"));

      System.setProperty(GradleRuntime.INSTALLATION_PROPERTY,
          temporaryDirectory.resolve("missing").toString());
      assertThrows(IllegalStateException.class, GradleRuntime::containerBinds);
    } finally {
      restoreProperty(GradleRuntime.INSTALLATION_PROPERTY, previousInstallation);
      restoreProperty(GradleRuntime.MODULE_CACHE_ARCHIVE_PROPERTY, previousCacheArchive);
    }
  }

  @Test
  void gradleCommandsUseTheIsolatedCacheAndOfflineMode() {
    final String extraction = GradleRuntime.cacheExtractionCommand();
    assertTrue(extraction.contains("jar -xf /opt/orchestra-gradle-modules.jar"));
    assertTrue(extraction.contains("cd /opt/orchestra-gradle-user-home"));
    assertTrue(extraction.contains("gradle-gradle-cli-main-*.jar"));
    assertTrue(extraction.contains("chmod 755 /opt/orchestra-gradle-launcher"));
    assertFalse(extraction.contains("/root/.gradle"));
    assertTrue(GradleRuntime.offlineArguments().contains("--offline"));
  }

  @Test
  void hibernateImageResolvesTestRuntimeAndProvesOfflineTaskGraph() {
    final List<String> steps = HibernateGradleRuntime.buildSteps(
        Paths.get("src/test/resources/hibernate_files"), "/app/hibernate-orm", "7.3.0");
    final String dockerfile = String.join("\n", steps);

    assertTrue(HibernateGradleRuntime.runtimeResolutionInitScript().contains("task.classpath.files"));
    assertTrue(dockerfile.contains("-Pdb=pg_amazon_ci"));
    assertTrue(dockerfile.contains("--offline"));
    assertTrue(dockerfile.contains("test --dry-run"));
    assertTrue(dockerfile.contains("ENV GRADLE_USER_HOME=/opt/orchestra-gradle-user-home"));
  }

  private static void restoreProperty(final String name, final String value) {
    if (value == null) {
      System.clearProperty(name);
    } else {
      System.setProperty(name, value);
    }
  }

  private static TestRegionalClusterInfo region(final String name, final String suffix) {
    final TestRegionalClusterInfo region = new TestRegionalClusterInfo();
    region.setRegion(name);
    region.setInstanceEndpointSuffix(suffix);
    return region;
  }

  private static Set<String> set(final String value) {
    return new HashSet<>(Collections.singletonList(value));
  }

  @Test
  void tagSelectionAppendsToExistingValues() {
    final Map<String, String> properties = new LinkedHashMap<>();
    properties.put("test-exclude-tags", "gdb");

    OrchestraTestPolicy.appendTag(properties, "test-exclude-tags", "caching");
    OrchestraTestPolicy.appendTag(properties, "test-include-tags", "caching");

    assertEquals("gdb,caching", properties.get("test-exclude-tags"));
    assertEquals("caching", properties.get("test-include-tags"));
  }
}
