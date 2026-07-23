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

package software.amazon.jdbc.plugin.readwritesplitting.source;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.plugin.readwritesplitting.GdbSettings;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;
import software.amazon.jdbc.util.Messages;

/**
 * {@link ReaderCandidateSource} for Global Database: topology hosts filtered by accessible regions
 * and, optionally, restricted to the home region. Reproduces the legacy
 * {@code GdbReadWriteSplittingPlugin.getReaderHostCandidates}.
 */
public class GdbRegionFilteredHostsCandidateSource implements ReaderCandidateSource {

  private final GdbSettings settings;

  public GdbRegionFilteredHostsCandidateSource(final GdbSettings settings) {
    this.settings = settings;
  }

  @Override
  public List<HostSpec> candidates(final RwSplitContext ctx) throws SQLException {
    List<HostSpec> candidates = ctx.pluginService().getHosts();

    if (this.settings.accessibleRegions() != null) {
      candidates = candidates.stream()
          .filter(this.settings::isInAccessibleRegion)
          .collect(Collectors.toList());
    }

    if (this.settings.restrictReaderToHomeRegion()) {
      final List<HostSpec> hostsInRegion = candidates.stream()
          .filter(this.settings::isInHomeRegion)
          .collect(Collectors.toList());

      if (hostsInRegion.isEmpty()) {
        ctx.logAndThrow(Messages.get(
            "GdbReadWriteSplittingPlugin.noAvailableReadersInHomeRegion",
            new Object[] {this.settings.homeRegion()}));
      }
      return hostsInRegion;
    }
    return candidates;
  }
}
