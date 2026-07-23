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

package software.amazon.jdbc.plugin.readwritesplitting.signal;

/**
 * The connection role a {@link RoutingSignal} resolves for a given JDBC call in the unified
 * read/write splitting plugin.
 *
 * <ul>
 *   <li>{@link #WRITER} — the call should run on the writer connection.</li>
 *   <li>{@link #READER} — the call should run on a reader connection.</li>
 *   <li>{@link #KEEP} — pin to the current connection (explicit {@code /*@keep* /} hint).</li>
 *   <li>{@link #NO_DECISION} — this method is not a routing trigger for this signal; the signal
 *       abstains and no switch is attempted (distinct from {@link #KEEP}, which actively pins).</li>
 * </ul>
 */
public enum TargetRole {
  WRITER,
  READER,
  KEEP,
  NO_DECISION
}
