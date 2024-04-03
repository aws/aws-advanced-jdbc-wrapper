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

public enum BlueGreenPhases {
  NOT_CREATED(0, false),
  CREATED(1, false),
  PREPARATION(2, true), // nodes are accessible
  IN_PROGRESS(3, true), // active phase; nodes are not accessible

  POST(4, true), // nodes are accessible; some change are still in progress
  COMPLETED(5, true); // all changes are completed

  private final int value;
  private final boolean activeSwitchover;

  BlueGreenPhases(final int newValue, final boolean activeSwitchover) {

    this.value = newValue;
    this.activeSwitchover = activeSwitchover;
  }

  public int getValue() {
    return this.value;
  }

  public boolean isActiveSwitchover() {
    return this.activeSwitchover;
  }
}
