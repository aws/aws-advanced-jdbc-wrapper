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

import software.amazon.orchestra.contract.Cache;

/**
 * Role tokens for the four Valkey caches the cache tests need.
 *
 * <p>Four rather than one because the tests address them positionally and expect a specific set of
 * capabilities at each index: {@code RemoteQueryCachePluginTests} and {@code SpringCachingTests} read
 * {@code getValkeyServerInfo().getInstances()} and select index 0 for authenticated plaintext, 1 for
 * anonymous plaintext, 2 for authenticated TLS and 3 for anonymous TLS. The list order below is that
 * contract, and {@code OrchestraEnvironmentInfo} assembles the list in exactly this order.
 *
 * <p>Declared here rather than reusing Orchestra's {@code PrimaryCache} and {@code SecondaryCache} for two
 * of them and inventing names for the others. A mixed set would leave the reader guessing which pair the
 * generic names referred to; naming all four for their capabilities makes the composition legible and the
 * index mapping checkable.
 *
 * <p>All four extend Orchestra's {@code Cache}, so anything that acts on every cache in a composition
 * without caring about roles still sees them.
 *
 * <p>In the {@code test} source set rather than {@code orchestraTest} for the same reason as
 * {@link TestShape}: the context identifies roles by fully-qualified class name, so the host that binds a
 * role and the in-container code that looks it up have to name the same token.
 */
public final class CacheRoles {

  private CacheRoles() {
  }

  /** Index 0: requires authentication, plaintext. */
  public interface AuthCache extends Cache {
  }

  /** Index 1: no authentication, plaintext. */
  public interface NoAuthCache extends Cache {
  }

  /** Index 2: requires authentication, TLS. */
  public interface TlsAuthCache extends Cache {
  }

  /** Index 3: no authentication, TLS. */
  public interface TlsNoAuthCache extends Cache {
  }
}
