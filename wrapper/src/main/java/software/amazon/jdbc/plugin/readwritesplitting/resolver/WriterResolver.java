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

package software.amazon.jdbc.plugin.readwritesplitting.resolver;

import java.sql.SQLException;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;

/**
 * Establishes (or selects) the writer connection ("how to reach the writer?"). Implementations
 * cover cluster topology, configured endpoints, and Global Database region rules.
 */
public interface WriterResolver {

  /**
   * Resolves the writer target. The returned {@link WriterResolution} is either
   * {@link WriterResolution.Kind#CONNECTED} (a writer connection was established) or
   * {@link WriterResolution.Kind#STAY} (remain on the current reader connection, e.g. Global Write
   * Forwarding).
   *
   * @param ctx the read/write splitting context
   * @return the writer resolution
   */
  WriterResolution resolveWriter(RwSplitContext ctx) throws SQLException;
}
