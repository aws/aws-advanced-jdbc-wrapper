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

package software.amazon.jdbc.plugin.bluegreen;

import java.util.HashMap;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.StringUtils;

public enum BlueGreenPhase {
  NOT_CREATED(0, false),
  CREATED(1, false),
  PREPARATION(2, true), // nodes are accessible
  IN_PROGRESS(3, true), // active phase; nodes are not accessible

  POST(4, true), // nodes are accessible; some change are still in progress
  COMPLETED(5, true); // all changes are completed

  private static final HashMap<String, BlueGreenPhase> blueGreenStatusMapping =
      new HashMap<String, BlueGreenPhase>() {
        {
          put("AVAILABLE", BlueGreenPhase.CREATED);
          put("SWITCHOVER_INITIATED", BlueGreenPhase.PREPARATION);
          put("SWITCHOVER_IN_PROGRESS", BlueGreenPhase.IN_PROGRESS);
          put("SWITCHOVER_IN_POST_PROCESSING", BlueGreenPhase.POST);
          put("SWITCHOVER_COMPLETED", BlueGreenPhase.COMPLETED);
        }
      };

  private final int value;
  private final boolean activeSwitchoverOrCompleted;

  BlueGreenPhase(final int value, final boolean activeSwitchoverOrCompleted) {

    this.value = value;
    this.activeSwitchoverOrCompleted = activeSwitchoverOrCompleted;
  }

  public static BlueGreenPhase parsePhase(final String value, final String version) {
    if (StringUtils.isNullOrEmpty(value)) {
      return BlueGreenPhase.NOT_CREATED;
    }

    // Version parameter may be used to identify a proper mapping.
    // For now lets assume that mapping is always the same.
    final BlueGreenPhase phase = blueGreenStatusMapping.get(value.toUpperCase());

    if (phase == null) {
      throw new IllegalArgumentException(Messages.get("bgd.unknownStatus", new Object[] {value}));
    }
    return phase;
  }

  public int getValue() {
    return this.value;
  }

  public boolean isActiveSwitchoverOrCompleted() {
    return this.activeSwitchoverOrCompleted;
  }
}
