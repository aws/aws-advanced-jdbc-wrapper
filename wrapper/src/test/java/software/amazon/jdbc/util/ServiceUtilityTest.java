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

package software.amazon.jdbc.util;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.ConnectionProvider;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.dialect.HostListProviderSupplier;
import software.amazon.jdbc.dialect.PgDialect;
import software.amazon.jdbc.hostlistprovider.HostListProvider;
import software.amazon.jdbc.profile.ConfigurationProfile;
import software.amazon.jdbc.profile.ConfigurationProfileBuilder;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.events.EventPublisher;
import software.amazon.jdbc.util.monitoring.MonitorService;
import software.amazon.jdbc.util.storage.StorageService;
import software.amazon.jdbc.util.telemetry.DefaultTelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

class ServiceUtilityTest {

  /**
   * Regression test for <a href="https://github.com/aws/aws-advanced-jdbc-wrapper/issues/2020">issue #2020</a>.
   *
   * <p>When a dialect is supplied via the {@link ConfigurationProfile}, {@code PluginServiceImpl} marks the dialect as
   * confirmed immediately. As a result, the {@code refreshHostList()} call inside
   * {@link ServiceUtility#createStandardServiceContainer} can eagerly create and cache the cluster topology monitor.
   * The monitor's minimal service container inherits its plugin list from this container's configuration profile via
   * {@code FullServicesContainer#getConfigurationProfile()}. If the profile has not yet been set on the container at
   * that point, the monitor silently falls back to the default plugin list (dropping profile-only plugins such as
   * {@code iam}) and its connections fail authentication once the cached IAM token expires.
   *
   * <p>This test verifies that the configuration profile is already available on the services container by the time
   * the host list provider is refreshed. A custom dialect supplies a host list provider that records the container's
   * configuration profile from within {@code refresh()} — the exact point at which the real
   * {@code RdsHostListProvider} would create the topology monitor.
   */
  @Test
  void createStandardServiceContainer_setsConfigurationProfileBeforeHostListRefresh() throws SQLException {
    final AtomicReference<@Nullable ConfigurationProfile> profileSeenAtRefresh = new AtomicReference<>();
    final AtomicBoolean refreshCalled = new AtomicBoolean(false);

    final ProfileCapturingDialect dialect = new ProfileCapturingDialect(profileSeenAtRefresh, refreshCalled);

    // Supplying a dialect through the profile makes isDialectConfirmed() true immediately, which allows the host list
    // to be refreshed (and the topology monitor to be created) during container construction. An empty plugin-factory
    // list keeps plugin instantiation minimal while still exercising the profile-based (non-string) plugin path.
    final ConfigurationProfile profile = ConfigurationProfileBuilder.get()
        .withName("issue-2020-test-profile")
        .withPluginFactories(Collections.emptyList())
        .withDialect(dialect)
        .build();

    final Properties props = new Properties();
    final TelemetryFactory telemetryFactory = new DefaultTelemetryFactory(props);

    final FullServicesContainer container = ServiceUtility.getInstance().createStandardServiceContainer(
        mock(StorageService.class),
        mock(MonitorService.class),
        mock(EventPublisher.class),
        mock(ConnectionProvider.class),
        null,
        telemetryFactory,
        "jdbc:postgresql://test-host:5432/db",
        "jdbc:postgresql://",
        mock(TargetDriverDialect.class),
        props,
        profile);

    assertTrue(refreshCalled.get(),
        "The host list provider should be refreshed during container creation.");
    assertSame(profile, profileSeenAtRefresh.get(),
        "The configuration profile must be set on the services container before the host list is refreshed, "
            + "otherwise an eagerly-created topology monitor would miss profile-only plugins such as iam.");
    assertSame(profile, container.getConfigurationProfile());
  }

  /**
   * A PostgreSQL dialect whose host list provider records the configuration profile visible on the services container
   * at the moment the host list is refreshed.
   */
  static class ProfileCapturingDialect extends PgDialect {
    private final AtomicReference<@Nullable ConfigurationProfile> profileSeenAtRefresh;
    private final AtomicBoolean refreshCalled;

    ProfileCapturingDialect(
        final AtomicReference<@Nullable ConfigurationProfile> profileSeenAtRefresh,
        final AtomicBoolean refreshCalled) {
      this.profileSeenAtRefresh = profileSeenAtRefresh;
      this.refreshCalled = refreshCalled;
    }

    @Override
    public HostListProviderSupplier getHostListProviderSupplier() {
      return (properties, initialUrl, servicesContainer) ->
          new ProfileCapturingHostListProvider(servicesContainer, this.profileSeenAtRefresh, this.refreshCalled);
    }
  }

  static class ProfileCapturingHostListProvider implements HostListProvider {
    private final FullServicesContainer servicesContainer;
    private final AtomicReference<@Nullable ConfigurationProfile> profileSeenAtRefresh;
    private final AtomicBoolean refreshCalled;

    ProfileCapturingHostListProvider(
        final FullServicesContainer servicesContainer,
        final AtomicReference<@Nullable ConfigurationProfile> profileSeenAtRefresh,
        final AtomicBoolean refreshCalled) {
      this.servicesContainer = servicesContainer;
      this.profileSeenAtRefresh = profileSeenAtRefresh;
      this.refreshCalled = refreshCalled;
    }

    private void capture() {
      this.refreshCalled.set(true);
      this.profileSeenAtRefresh.set(this.servicesContainer.getConfigurationProfile());
    }

    @Override
    public @Nullable List<HostSpec> getCurrentTopology(final Connection conn, final HostSpec initialHostSpec) {
      return null;
    }

    @Override
    public List<HostSpec> refresh() {
      capture();
      return new ArrayList<>();
    }

    @Override
    public @Nullable List<HostSpec> forceRefresh() {
      capture();
      return new ArrayList<>();
    }

    @Override
    public @Nullable List<HostSpec> forceRefresh(final boolean verifyTopology, final long timeoutMs) {
      capture();
      return new ArrayList<>();
    }

    @Override
    public String getClusterId() {
      return "issue-2020-test-cluster";
    }

    @Override
    public void stopMonitor() {
      // no-op
    }
  }
}
