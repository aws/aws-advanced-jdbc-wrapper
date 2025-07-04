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

package software.amazon.jdbc.states;

import java.util.HashMap;
import java.util.Map;

public class SessionState {
  public SessionStateField<Boolean> autoCommit = new SessionStateField<>();
  public SessionStateField<Boolean> readOnly = new SessionStateField<>();
  public SessionStateField<String> catalog = new SessionStateField<>();
  public SessionStateField<String> schema = new SessionStateField<>();
  public SessionStateField<Integer> holdability = new SessionStateField<>();
  public SessionStateField<Integer> networkTimeout = new SessionStateField<>();
  public SessionStateField<Integer> transactionIsolation = new SessionStateField<>();
  public SessionStateField<Map<String, Class<?>>> typeMap = new SessionStateField<>();

  public SessionState copy() {
    final SessionState newSessionState = new SessionState();
    newSessionState.autoCommit = this.autoCommit.copy();
    newSessionState.readOnly = this.readOnly.copy();
    newSessionState.catalog = this.catalog.copy();
    newSessionState.schema = this.schema.copy();
    newSessionState.holdability = this.holdability.copy();
    newSessionState.networkTimeout = this.networkTimeout.copy();
    newSessionState.transactionIsolation = this.transactionIsolation.copy();

    // typeMap requires a special care since it uses map, and it needs to be properly cloned.
    if (this.typeMap.getValue().isPresent()) {
      newSessionState.typeMap.setValue(new HashMap<>(this.typeMap.getValue().get()));
    }
    if (this.typeMap.getPristineValue().isPresent()) {
      newSessionState.typeMap.setPristineValue(new HashMap<>(this.typeMap.getPristineValue().get()));
    }

    return newSessionState;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("autoCommit: " + autoCommit + "\n");
    sb.append("readOnly: " + readOnly + "\n");
    sb.append("catalog: " + catalog + "\n");
    sb.append("schema: " + schema + "\n");
    sb.append("holdability: " + holdability + "\n");
    sb.append("networkTimeout: " + networkTimeout + "\n");
    sb.append("transactionIsolation: " + transactionIsolation + "\n");
    sb.append("typeMap: " + typeMap + "\n");
    return sb.toString();
  }
}
