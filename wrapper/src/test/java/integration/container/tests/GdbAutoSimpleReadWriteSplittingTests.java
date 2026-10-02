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

package integration.container.tests;

import integration.container.TestDriverProvider;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * The fourth corner of the matrix: endpoints instead of topology, SQL instead of {@code setReadOnly}.
 *
 * <p>Nothing here but the plugin code. Everything else is inherited from
 * {@link GdbSimpleReadWriteSplittingTests}, which is the point - the two axes the four plugins vary along are
 * meant to be independent, and a subclass that needed its own assertions would be evidence they are not.
 */
@ExtendWith(TestDriverProvider.class)
@Order(43)
public class GdbAutoSimpleReadWriteSplittingTests extends GdbSimpleReadWriteSplittingTests {

  {
    this.pluginCode = "sqlParser,gdbAutoSimpleReadWriteSplitting";
  }
}
