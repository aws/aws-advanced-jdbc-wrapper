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

package integration;

/**
 * JUnit tag names that both the test classes and the host-side runners need to agree on.
 *
 * <p>A constant rather than a literal in two places. A tag is how a run says "these classes and not the others",
 * so a typo does not fail - it silently selects nothing, or selects everything, and the run reports success
 * either way. Sharing the name is what makes that impossible.
 *
 * <p>In the container-visible source set because both sides use it: the classes carry it in {@code @Tag}, and the
 * host passes it as {@code test-include-tags} when it provisions the environment those classes need.
 */
public final class TestTags {

  /**
   * Tests that need an Aurora global database.
   *
   * <p>Excluded from the ordinary suites and included by a global database run. Both directions are needed: a GDB
   * class in a single-region run would have no second region to test against, and the single-region classes in a
   * GDB run would spend an hour re-checking local behaviour against a topology built for something else - and
   * the ones that assume a writer in the region they are connected to would fail.
   */
  public static final String GDB = "gdb";

  private TestTags() {
  }
}
