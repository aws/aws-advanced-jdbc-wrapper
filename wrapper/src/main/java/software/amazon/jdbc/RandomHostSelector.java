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

package software.amazon.jdbc;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import software.amazon.jdbc.util.Messages;

public class RandomHostSelector implements HostSelector {

  @Override
  public HostSpec getHost(List<HostSpec> hosts, HostRole role) throws SQLException {
    List<HostSpec> eligibleHosts = hosts.stream()
        .filter(hostSpec -> role.equals(hostSpec.getRole())).collect(Collectors.toList());
    if (eligibleHosts.size() == 0) {
      throw new SQLException(Messages.get("RandomHostSelector.noHostsMatchingRole", new Object[]{role}));
    }

    int randomIndex = (int) (Math.random() * eligibleHosts.size());
    return eligibleHosts.get(randomIndex);
  }

  @Override
  public List<HostSpec> getHostsByPriority(List<HostSpec> hosts, HostRole role) throws SQLException {
    List<HostSpec> eligibleHosts = hosts.stream()
        .filter(hostSpec -> role.equals(hostSpec.getRole())).collect(Collectors.toList());
    if (eligibleHosts.size() == 0) {
      throw new SQLException(Messages.get("RandomHostSelector.noHostsMatchingRole", new Object[]{role}));
    }

    Collections.shuffle(eligibleHosts);
    return eligibleHosts;
  }

  @Override
  public List<HostSpec> getHostsByPriority(List<HostSpec> hosts, List<HostSpec> excludeList, HostRole role) throws
      SQLException {
    Set<String> excludeUrls = new HashSet<>();
    for (HostSpec hostSpec : excludeList) {
      excludeUrls.add(hostSpec.getUrl());
    }

    List<HostSpec> eligibleHosts = hosts.stream()
        .filter(h -> role.equals(h.getRole()) && !excludeUrls.contains(h.getUrl()))
        .collect(Collectors.toList());
    if (eligibleHosts.size() == 0) {
      throw new SQLException(Messages.get("RandomHostSelector.noHostsMatchingCriteria", new Object[]{role}));
    }

    Collections.shuffle(eligibleHosts);
    return eligibleHosts;
  }
}
