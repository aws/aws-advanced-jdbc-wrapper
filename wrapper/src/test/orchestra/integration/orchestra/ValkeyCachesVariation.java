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

package integration.orchestra;

import integration.orchestra.CacheRoles.AuthCache;
import integration.orchestra.CacheRoles.NoAuthCache;
import integration.orchestra.CacheRoles.TlsAuthCache;
import integration.orchestra.CacheRoles.TlsNoAuthCache;
import java.util.ArrayList;
import java.util.List;
import software.amazon.orchestra.Composition;
import software.amazon.orchestra.Variation;
import software.amazon.orchestra.instruments.docker.ValkeyContainerConfiguration;
import software.amazon.orchestra.instruments.docker.ValkeyContainerInstrumentDefinition;

/**
 * Adds the four Valkey caches the cache tests need, each configured differently.
 *
 * <p>Not an axis: it returns one composition per input rather than multiplying them. It is a {@code Variation}
 * because that is the only place Orchestra exposes role-scoped configuration - {@code EnvCompositionBuilder}
 * can bind a role but not override configuration for one - and four caches of the same class differing only
 * in authentication and TLS is exactly what role-scoped overrides are for.
 *
 * <p>The alternative would be four configuration interfaces or four instrument subclasses, both of which
 * would encode "which cache" into a type just to carry two booleans.
 *
 * <p>Order is not incidental. {@code RemoteQueryCachePluginTests} and {@code SpringCachingTests} select
 * caches by index, so the roles are bound in the order {@link CacheRoles} documents and
 * {@code OrchestraEnvironmentInfo} reassembles the list the same way.
 */
public class ValkeyCachesVariation implements Variation {

  /**
   * The cache user, which must match {@code valkey-acl.conf}.
   *
   * <p>Duplicated from the ACL file rather than parsed out of it. The file is Valkey's format, not a key-value
   * list, and a parser for two fields would be a way to be subtly wrong; the coupling is instead stated here so
   * a change to one is an obvious prompt to change the other. The values also match what the harness published,
   * so the in-container tests' expectations are unchanged.
   */
  private static final String VALKEY_USERNAME = "test_cache_user";

  /** That user's password, likewise matching {@code valkey-acl.conf}. */
  private static final String VALKEY_PASSWORD = "test_cache_password";

  /**
   * Returns the four caches configured the way the wrapper's cache tests expect.
   *
   * <p>A factory rather than six arguments at each call site, because two compositions need the same four
   * caches - an Aurora run and a Docker one - and the harness ran the cache tests only against Docker while
   * this repository's Aurora composition has carried them from the start. Constructing them twice by hand is
   * how the two would drift into configuring different caches under the same role names.
   *
   * @return the variation, reading its fixtures from the test resources
   */
  public static ValkeyCachesVariation standard() {
    return new ValkeyCachesVariation(
        resource("valkey-acl.conf"),
        VALKEY_USERNAME,
        VALKEY_PASSWORD,
        resource("certs/valkey.crt"),
        resource("certs/valkey.key"),
        resource("certs/ca.crt"));
  }

  /**
   * Reads a test fixture as a string.
   *
   * <p>Contents rather than a path, because that is what the cache instrument takes: the files exist only to
   * configure a container, so requiring them on disk inside it buys nothing.
   */
  private static String resource(final String name) {
    final java.nio.file.Path path = java.nio.file.Path.of("src/test/resources").resolve(name);
    try {
      return java.nio.file.Files.readString(path);
    } catch (final java.io.IOException e) {
      throw new java.io.UncheckedIOException(
          "Could not read the test fixture " + path.toAbsolutePath()
              + ", which the Valkey caches need in order to be configured.", e);
    }
  }

  /**
   * The network alias prefix, suffixed with the cache's index.
   *
   * <p>Dictated by the TLS certificate, not chosen. {@code certs/valkey.crt} carries
   * {@code subjectAltName=DNS:localhost,DNS:valkey-server-address-0..3}, and a modern TLS client verifies
   * the hostname against those SANs - so a cache reached by any other name fails the handshake. Renaming
   * these would mean regenerating the certificate fixture, which is a much larger change than it looks.
   *
   * <p>It also keeps the names the harness used, so anything that recognises them still does.
   */
  private static final String ALIAS_PREFIX = "valkey-server-address-";

  /** The ACL file contents, defining the user the authenticated caches accept. */
  private final String aclFile;

  private final String username;
  private final String password;
  private final String certificate;
  private final String privateKey;
  private final String caCertificate;

  /**
   * Creates the variation.
   *
   * @param aclFile the Valkey ACL file contents granting {@code username} access
   * @param username the user the authenticated caches accept
   * @param password that user's password
   * @param certificate the server certificate, in PEM, for the TLS caches
   * @param privateKey the matching private key, in PEM
   * @param caCertificate the CA certificate, in PEM
   */
  public ValkeyCachesVariation(
      final String aclFile,
      final String username,
      final String password,
      final String certificate,
      final String privateKey,
      final String caCertificate) {

    this.aclFile = aclFile;
    this.username = username;
    this.password = password;
    this.certificate = certificate;
    this.privateKey = privateKey;
    this.caCertificate = caCertificate;
  }

  @Override
  public List<Composition> process(final List<Composition> compositions) {
    final List<Composition> configured = new ArrayList<>(compositions.size());

    for (final Composition composition : compositions) {
      Composition result = composition;

      // A fresh definition instance per role. The definition keys its containers by composition display
      // name, so four roles sharing one instance would overwrite each other's entry and tear down one
      // container four times.
      result = withCache(result, AuthCache.class, ALIAS_PREFIX + "0", true, false);
      result = withCache(result, NoAuthCache.class, ALIAS_PREFIX + "1", false, false);
      result = withCache(result, TlsAuthCache.class, ALIAS_PREFIX + "2", true, true);
      result = withCache(result, TlsNoAuthCache.class, ALIAS_PREFIX + "3", false, true);

      configured.add(result);
    }
    return configured;
  }

  /**
   * Binds one cache and the configuration that distinguishes it.
   *
   * <p>The alias is overridden per role rather than left to Orchestra's automatic disambiguation. That
   * mechanism would produce unique but arbitrary names, and these appear in the endpoints tests connect to
   * and in gateway rules, so a name that says what the cache is makes a failure legible.
   */
  private Composition withCache(
      final Composition composition,
      final Class<?> role,
      final String alias,
      final boolean authenticated,
      final boolean tls) {

    Composition result = composition
        .withInstrument(role, new ValkeyContainerInstrumentDefinition())
        .withOverride(role, ValkeyContainerConfiguration.class, "getValkeyNetworkAlias", alias);

    if (authenticated) {
      result = result
          .withOverride(role, ValkeyContainerConfiguration.class, "getValkeyAclFile", this.aclFile)
          .withOverride(role, ValkeyContainerConfiguration.class, "getValkeyUsername", this.username)
          .withOverride(role, ValkeyContainerConfiguration.class, "getValkeyPassword", this.password);
    }

    if (tls) {
      result = result
          .withOverride(role, ValkeyContainerConfiguration.class, "getValkeyTlsCertificate", this.certificate)
          .withOverride(role, ValkeyContainerConfiguration.class, "getValkeyTlsPrivateKey", this.privateKey)
          .withOverride(
              role, ValkeyContainerConfiguration.class, "getValkeyTlsCaCertificate", this.caCertificate);
    }

    return result;
  }
}
