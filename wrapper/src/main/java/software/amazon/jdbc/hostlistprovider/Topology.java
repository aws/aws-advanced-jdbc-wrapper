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

package software.amazon.jdbc.hostlistprovider;

import java.util.List;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostSpec;

public class Topology {
  private final @NonNull List<HostSpec> hosts;

  public Topology(@NonNull List<HostSpec> hosts) {
    this.hosts = hosts;
  }

  public @NonNull List<HostSpec> getHosts() {
    return hosts;
  }

  @Override
  public int hashCode() {
    return Objects.hash(hosts);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    Topology other = (Topology) obj;
    return Objects.equals(hosts, other.hosts);
  }
}
