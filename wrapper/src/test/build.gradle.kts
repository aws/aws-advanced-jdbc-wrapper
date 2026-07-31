/*
*    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
*
*    Licensed under the Apache License, Version 2.0 (the "License").
*    You may not use this file except in compliance with the License.
*    You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
*    Unless required by applicable law or agreed to in writing, software
*    distributed under the License is distributed on an "AS IS" BASIS,
*    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*    See the License for the specific language governing permissions and
*    limitations under the License.
*/

import org.gradle.api.tasks.testing.logging.TestExceptionFormat.*
import org.gradle.api.tasks.testing.logging.TestLogEvent.*

plugins {
    java
}

repositories {
    mavenCentral()
}

dependencies {
    testImplementation("org.junit.platform:junit-platform-commons:1.11.3")
    testImplementation("org.junit.platform:junit-platform-engine:1.11.0")
    testImplementation("org.junit.platform:junit-platform-launcher:1.11.3")
    testImplementation("org.junit.platform:junit-platform-suite-engine:1.11.3")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.11.3")
    testImplementation("org.junit.jupiter:junit-jupiter-params:5.10.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")

    testImplementation("org.apache.commons:commons-dbcp2:2.12.0")
    testImplementation("org.postgresql:postgresql:42.7.10")
    testImplementation("com.mysql:mysql-connector-j:9.1.0")
    testImplementation("org.mariadb.jdbc:mariadb-java-client:3.5.6")
    testImplementation("com.zaxxer:HikariCP:4.0.3") // Version 4.+ is compatible with Java 8
    testImplementation("org.springframework.boot:spring-boot-starter-jdbc:2.7.13") // 2.7.13 is the last version compatible with Java 8
    testImplementation("org.mockito:mockito-inline:4.11.0") // 4.11.0 is the last version compatible with Java 8
    testImplementation("software.amazon.awssdk:ec2:2.42.38")
    testImplementation("software.amazon.awssdk:rds:2.42.38")
    testImplementation("software.amazon.awssdk:sts:2.42.38")
    testImplementation("software.amazon.awssdk:secretsmanager:2.42.38")
    // Note: all org.testcontainers dependencies should have the same version
    testImplementation("org.testcontainers:testcontainers:1.20.4")
    testImplementation("org.testcontainers:mysql:1.20.4")
    testImplementation("org.testcontainers:postgresql:1.20.4")
    testImplementation("org.testcontainers:mariadb:1.20.4")
    testImplementation("org.testcontainers:junit-jupiter:1.20.4")
    testImplementation("org.testcontainers:toxiproxy:1.20.4")
    testImplementation("org.apache.commons:commons-pool2:2.11.1")
    testImplementation("org.apache.poi:poi-ooxml:5.3.0")
    testImplementation("org.slf4j:slf4j-simple:2.0.13")
    testImplementation("com.fasterxml.jackson.core:jackson-databind:2.17.1")
    testImplementation("com.amazonaws:aws-xray-recorder-sdk-core:2.18.2")
    testImplementation("io.opentelemetry:opentelemetry-sdk:1.42.1")
    testImplementation("io.opentelemetry:opentelemetry-sdk-metrics:1.43.0")
    testImplementation("io.opentelemetry:opentelemetry-exporter-otlp:1.44.1")
    testImplementation("de.vandermeer:asciitable:0.3.2")
    testImplementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.19.2")
    testImplementation("com.github.jsqlparser:jsqlparser:4.9")
    // XA transaction managers for XADataSource integration tests (both are exercised for
    // compatibility). Must match the host build file (wrapper/build.gradle.kts). Narayana 5.11.x is
    // the last Java 8-compatible line.
    testImplementation("org.jboss.narayana.jta:narayana-jta:5.11.4.Final")
    // Narayana declares jboss-logging as an optional/provided dependency, so it is not pulled
    // transitively; add it explicitly or Narayana's jtaLogger fails to initialize at runtime.
    testImplementation("org.jboss.logging:jboss-logging:3.4.3.Final")
    testImplementation("com.atomikos:transactions-jta:5.0.9")
    testImplementation("com.atomikos:transactions-jdbc:5.0.9")
    val arch = System.getProperty("os.arch").let {
        when (it) {
            "aarch64", "arm64" -> "aarch_64"
            else -> "x86_64"
        }
    }
    val isMusl = try {
        val process = ProcessBuilder("ldd", "--version").redirectErrorStream(true).start()
        val output = process.inputStream.bufferedReader().readText()
        process.waitFor()
        output.contains("musl")
    } catch (e: Exception) {
        // If ldd doesn't exist, check for Alpine marker
        File("/etc/alpine-release").exists()
    }
    val glideClassifier = if (isMusl) "linux_musl-$arch" else "linux-$arch"
    testImplementation("io.valkey:valkey-glide:2.3.0:$glideClassifier")
}

// Hibernate v7.3 requires at least Java 17
// Create a separate source set for Hibernate tests compiled with Java 17
val hibernateTest: SourceSet by sourceSets.creating {
    java {
        srcDir("java17")
    }
    compileClasspath += sourceSets.test.get().output + sourceSets.test.get().compileClasspath
    runtimeClasspath += sourceSets.test.get().output + sourceSets.test.get().runtimeClasspath
}

tasks.named<JavaCompile>(hibernateTest.compileJavaTaskName) {
    javaCompiler.set(javaToolchains.compilerFor {
        languageVersion.set(JavaLanguageVersion.of(17))
    })
    options.release.set(17)
    dependsOn(tasks.compileTestJava)
}

dependencies {
    // Hibernate test dependencies (Java 17+)
    add(hibernateTest.implementationConfigurationName, "org.hibernate.orm:hibernate-core:7.4.1.Final")
    add(hibernateTest.implementationConfigurationName, "jakarta.persistence:jakarta.persistence-api:3.2.0")
}

tasks.withType<Test> {
    dependsOn(tasks.named(hibernateTest.compileJavaTaskName))

    testClassesDirs += fileTree("./libs") { include("*.jar") } + project.files("./test") + hibernateTest.output.classesDirs
    classpath += fileTree("./libs") { include("*.jar") } + project.files("./test") + project.files("./test/resources") + hibernateTest.output
    outputs.upToDateWhen { false }

    useJUnitPlatform {
        System.getProperty("test-include-tags")?.split(",")?.forEach { tag ->
            includeTags(tag)
            println("Include tests with tag: $tag")
        }
        System.getProperty("test-exclude-tags")?.split(",")?.forEach { tag ->
            excludeTags(tag)
            println("Exclude tests with tag: $tag")
        }
    }

    testLogging {
        events(PASSED, FAILED, SKIPPED)
        showStandardStreams = true
        exceptionFormat = FULL
        showExceptions = true
        showCauses = true
        showStackTraces = true
    }

    systemProperty("java.util.logging.config.file", "./test/resources/logging-test.properties")
    systemProperty("junit.jupiter.params.displayname.default", "{displayName} - {arguments}")

    reports.junitXml.required.set(true)
    reports.junitXml.outputLocation.set(file("${project.layout.buildDirectory.get()}/test-results/container-" + System.currentTimeMillis()))

    reports.html.required.set(false)
}

// Approximate cost of each integration test class, in seconds, derived from measured CI runs.
// Used ONLY to balance test classes across shards - it never affects which tests run, so a stale
// or missing entry costs some balance but can never drop coverage.
//
// Each value is the worst per-environment cost observed for that class, taken as the maximum over
// the most recent runs so that shards stay balanced for the slowest environment. Refresh these when
// a class's runtime changes substantially; run 30645460736 showed that under-weighting a class by
// 60% is enough to make its shard the critical path.
val testClassWeightsSeconds = mapOf(
    "FailoverTest" to 1136,
    "AutoReadWriteSplittingTests" to 1101,
    "GdbFailoverTest" to 1100,
    "SimpleReadWriteSplittingTests" to 1095,
    "AutoSimpleReadWriteSplittingTests" to 995,
    "Failover2Test" to 971,
    "ReadWriteSplittingTests" to 921,
    "CustomEndpointTest" to 516,
    "DataCachePluginTests" to 456,
    "XaFailoverTest" to 349,
    "HikariTests" to 238,
    "AuroraInitialConnectionStrategyTest" to 145,
    "XaTransactionTest" to 90,
    "FastestResponseStrategyTest" to 82,
    "BasicConnectivityTests" to 70,
    "AwsIamIntegrationTest" to 61,
    "AwsSecretsManager2IntegrationTest" to 61,
    "EFM2Test" to 48,
    "XaTwoPhaseCommitTest" to 47,
    "LogQueryPluginTests" to 22,
    "XaIamAuthenticationTest" to 20,
    "DriverConfigurationProfileTests" to 16,
    "DataSourceTests" to 13,
    "SpringTests" to 12,
    "RdsConnectivityTests" to 11,
    // Gated off by deployment/feature conditions in the sharded Aurora and Multi-AZ workflows, so
    // they cost nothing there. They still run (unsharded) in their own dedicated workflows, where
    // these weights are unused.
    "AdvancedPerformanceTest" to 0,
    "AutoscalingTests" to 0,
    "BlueGreenDeploymentTests" to 0,
    "DatabasePerformanceMetricTest" to 0,
    "KmsEncryptionIntegrationTest" to 0,
    "PerformanceTest" to 0,
    "ReadWriteSplittingPerformanceTest" to 0,
    "RemoteQueryCachePluginTests" to 0,
    "SpringCachingTests" to 0
)

// Any class missing from the table above still runs; it is just assumed to be moderately
// expensive so that a newly added test cannot quietly unbalance a shard by a large amount.
val defaultTestClassWeightSeconds = 60

// Classes that live under integration.container.tests but hold no tests. They are listed
// explicitly so that any *other* class not following the Test/Tests naming convention fails the
// build instead of quietly never being assigned to a shard.
val nonTestHelperClasses = setOf(
    "integration.container.tests.metrics.FailoverResult",
    "integration.container.tests.metrics.RunData",
    "integration.container.tests.metrics.RunDataNode",
    "integration.container.tests.metrics.RunDataRow",
    "integration.container.tests.metrics.Runs",
    "integration.container.tests.metrics.TopologyEventHolder"
)

/**
 * Returns the fully qualified names of every compiled class under integration.container.tests.
 *
 * The universe is read from disk rather than from a hardcoded list so that a newly added test
 * class is always picked up by exactly one shard instead of being silently skipped.
 */
fun discoverContainerTestClasses(): List<String> {
    val classesRoot = file("./test")
    val testsPackageDir = file("./test/integration/container/tests")
    if (!testsPackageDir.isDirectory) {
        return emptyList()
    }
    return testsPackageDir.walkTopDown()
        .filter { it.isFile && it.name.endsWith(".class") && !it.name.contains('$') }
        .map {
            it.relativeTo(classesRoot).path
                .removeSuffix(".class")
                .replace('\\', '.')
                .replace('/', '.')
        }
        .sorted()
        .toList()
}

/**
 * Assigns classes to [shardCount] shards with a longest-processing-time-first pass and returns the
 * ones belonging to [shardIndex] (1-based). Every class lands in exactly one shard, and the result
 * depends only on the class list and the weight table, so all shards of a run agree on the split
 * without needing to talk to each other.
 */
fun selectShard(classNames: List<String>, shardIndex: Int, shardCount: Int): List<String> {
    val shardTotals = LongArray(shardCount)
    val shards = List(shardCount) { mutableListOf<String>() }
    val ordered = classNames.sortedWith(
        // Heaviest first, then by name so ties are broken deterministically.
        compareByDescending<String> {
            testClassWeightsSeconds[it.substringAfterLast('.')] ?: defaultTestClassWeightSeconds
        }.thenBy { it }
    )
    for (className in ordered) {
        val weight = testClassWeightsSeconds[className.substringAfterLast('.')]
            ?: defaultTestClassWeightSeconds
        var target = 0
        for (i in 1 until shardCount) {
            if (shardTotals[i] < shardTotals[target]) {
                target = i
            }
        }
        shards[target].add(className)
        shardTotals[target] += weight.toLong()
    }
    return shards[shardIndex - 1].sorted()
}

tasks.register<Test>("in-container") {
    filter.excludeTestsMatching("software.*") // exclude unit tests

    val shardIndex = (System.getProperty("test-shard-index") ?: "1").toInt()
    val shardCount = (System.getProperty("test-shard-count") ?: "1").toInt()

    if (shardCount <= 1) {
        // modify below filter to select specific integration tests
        // see https://docs.gradle.org/current/javadoc/org/gradle/api/tasks/testing/TestFilter.html
        filter.includeTestsMatching("integration.container.tests.*")
    } else {
        require(shardIndex in 1..shardCount) {
            "test-shard-index must be between 1 and test-shard-count ($shardCount), got $shardIndex"
        }
        val discovered = discoverContainerTestClasses()
        require(discovered.isNotEmpty()) {
            "Sharding was requested but no compiled classes were found under " +
                "integration.container.tests. Is ./test populated?"
        }
        val unclassified = discovered.filter {
            !nonTestHelperClasses.contains(it) && !it.endsWith("Test") && !it.endsWith("Tests")
        }
        require(unclassified.isEmpty()) {
            "Cannot shard: $unclassified neither follow the Test/Tests naming convention nor appear " +
                "in nonTestHelperClasses, so it is unclear whether they must be run. Rename them or " +
                "add them to nonTestHelperClasses in wrapper/src/test/build.gradle.kts."
        }
        val allClasses = discovered.filter { !nonTestHelperClasses.contains(it) }
        require(allClasses.size >= shardCount) {
            "test-shard-count ($shardCount) exceeds the number of discovered test classes " +
                "(${allClasses.size}); some shards would have nothing to run."
        }
        val shardClasses = selectShard(allClasses, shardIndex, shardCount)
        println("Test shard $shardIndex of $shardCount: ${shardClasses.size} of ${allClasses.size} classes")
        shardClasses.forEach { println("  $it") }
        shardClasses.forEach { filter.includeTestsMatching(it) }
    }
}
