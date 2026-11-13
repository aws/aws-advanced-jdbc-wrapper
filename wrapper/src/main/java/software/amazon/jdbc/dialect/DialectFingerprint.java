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

package software.amazon.jdbc.dialect;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Immutable set of server-side facts collected from a single connection, from which the
 * database dialect is derived. Every field is an observation (an object exists, a variable has a
 * value), never a conclusion; the conclusion is drawn by {@link DialectDetector#resolve}.
 */
public final class DialectFingerprint {

  /** Engine family. Selects which fingerprint queries are run and which decision table applies. */
  public enum Family {
    PG,
    MYSQL,
    UNKNOWN
  }

  /** Value of {@link #getGlobalDbRegionCount()} when the region count was not queried. */
  public static final int REGION_COUNT_NOT_QUERIED = -1;

  private final Family family;
  private final @Nullable String serverVersion;
  private final boolean mariaDb;
  private final boolean aurora;
  private final boolean limitless;
  private final boolean globalDbCapable;
  private final int globalDbRegionCount;
  private final boolean rds;
  private final boolean multiAzClusterCapable;
  private final boolean multiAzCluster;

  private DialectFingerprint(final Builder b) {
    this.family = b.family;
    this.serverVersion = b.serverVersion;
    this.mariaDb = b.mariaDb;
    this.aurora = b.aurora;
    this.limitless = b.limitless;
    this.globalDbCapable = b.globalDbCapable;
    this.globalDbRegionCount = b.globalDbRegionCount;
    this.rds = b.rds;
    this.multiAzClusterCapable = b.multiAzClusterCapable;
    this.multiAzCluster = b.multiAzCluster;
  }

  public static Builder builder(final Family family) {
    return new Builder(family);
  }

  public Family getFamily() {
    return this.family;
  }

  /** Server version string as reported by the engine, or null if not collected. */
  public @Nullable String getServerVersion() {
    return this.serverVersion;
  }

  /** The server version identifies itself as MariaDB. */
  public boolean isMariaDb() {
    return this.mariaDb;
  }

  /** An Aurora-only object exists ({@code aurora_version} variable or function). */
  public boolean isAurora() {
    return this.aurora;
  }

  /** The Aurora Limitless router function exists. */
  public boolean isLimitless() {
    return this.limitless;
  }

  /** The Aurora Global Database status object exists (true on every recent Aurora engine). */
  public boolean isGlobalDbCapable() {
    return this.globalDbCapable;
  }

  /** Number of regions reported by the global database status, or {@link #REGION_COUNT_NOT_QUERIED}. */
  public int getGlobalDbRegionCount() {
    return this.globalDbRegionCount;
  }

  /** An RDS-installed object exists ({@code rds.*} GUC, {@code mysql.rds_*} objects). */
  public boolean isRds() {
    return this.rds;
  }

  /** The object used to confirm a Multi-AZ DB cluster exists, so the confirmation was attempted. */
  public boolean isMultiAzClusterCapable() {
    return this.multiAzClusterCapable;
  }

  /** The Multi-AZ DB cluster confirmation succeeded. */
  public boolean isMultiAzCluster() {
    return this.multiAzCluster;
  }

  @Override
  public String toString() {
    return "DialectFingerprint["
        + "family=" + this.family
        + ", serverVersion=" + this.serverVersion
        + ", mariaDb=" + this.mariaDb
        + ", aurora=" + this.aurora
        + ", limitless=" + this.limitless
        + ", globalDbCapable=" + this.globalDbCapable
        + ", globalDbRegionCount=" + this.globalDbRegionCount
        + ", rds=" + this.rds
        + ", multiAzClusterCapable=" + this.multiAzClusterCapable
        + ", multiAzCluster=" + this.multiAzCluster
        + "]";
  }

  public static final class Builder {
    private final Family family;
    private @Nullable String serverVersion;
    private boolean mariaDb;
    private boolean aurora;
    private boolean limitless;
    private boolean globalDbCapable;
    private int globalDbRegionCount = REGION_COUNT_NOT_QUERIED;
    private boolean rds;
    private boolean multiAzClusterCapable;
    private boolean multiAzCluster;

    private Builder(final Family family) {
      this.family = family;
    }

    public Builder serverVersion(final @Nullable String value) {
      this.serverVersion = value;
      return this;
    }

    public Builder mariaDb(final boolean value) {
      this.mariaDb = value;
      return this;
    }

    public Builder aurora(final boolean value) {
      this.aurora = value;
      return this;
    }

    public Builder limitless(final boolean value) {
      this.limitless = value;
      return this;
    }

    public Builder globalDbCapable(final boolean value) {
      this.globalDbCapable = value;
      return this;
    }

    public Builder globalDbRegionCount(final int value) {
      this.globalDbRegionCount = value;
      return this;
    }

    public Builder rds(final boolean value) {
      this.rds = value;
      return this;
    }

    public Builder multiAzClusterCapable(final boolean value) {
      this.multiAzClusterCapable = value;
      return this;
    }

    public Builder multiAzCluster(final boolean value) {
      this.multiAzCluster = value;
      return this;
    }

    public DialectFingerprint build() {
      return new DialectFingerprint(this);
    }
  }
}
