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

package integration.orchestra;

/**
 * Role token for the description of what kind of environment a run is exercising.
 *
 * <p>Orchestra publishes what a composition <em>contains</em> — endpoints, credentials, topology. It does
 * not publish what this suite calls a {@code TestEnvironmentRequest}: the engine, the deployment kind, the
 * instance count, and the {@code TestEnvironmentFeatures} set. Roughly 150 in-container call sites read those
 * through {@code getRequest()}, and the {@code @EnableOnDatabaseEngineDeployment} and
 * {@code @DisableOnTestFeature} conditions gate whole test classes on them.
 *
 * <p>So a small instrument publishes them. That is an intermediate state rather than a permanent design:
 * most of what a "feature" expresses is <em>which tests to run</em>, and Orchestra's answer to selection is
 * variations and tag filters, not a flag a test reads at runtime.
 *
 * <p>It outlived the migration deliberately. Converting the conditions means rewriting how the in-container
 * framework selects tests, across ~150 call sites and every {@code @EnableOn*} annotation - which is a change
 * to this suite's own test framework, not to how its environments are built. The provisioning migration is
 * complete with this in place, so the two are separable, and doing them together would have made a
 * provisioning failure and a test-selection failure indistinguishable.
 *
 * <p>In the {@code test} source set rather than the host-only {@code orchestraTest} one, because the context
 * identifies roles by fully-qualified class name and both sides have to name the same token: the host binds
 * it, the in-container facade looks it up.
 */
public interface TestShape {
}
