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

import org.checkerframework.gradle.plugin.CheckerFrameworkExtension

plugins {
    checkstyle
    java
    id("biz.aQute.bnd.builder")
    id("com.diffplug.spotless") version "6.13.0" // 6.13.0 is the last version that is compatible with Java 8
    id("com.github.spotbugs")
    id("com.github.vlsi.gradle-extensions")
    id("com.github.vlsi.ide")
    id("com.kncept.junit.reporter")
    id("com.gradleup.shadow") version "8.3.11" // 8.3.11 is the last version that is compatible with Java 8
    id("com.google.osdetector") version "1.7.3" // Plugin used to detect OS and use for GLIDE
    // Checker Framework: applied only when -PenableCheckerFramework is set (see guarded block below).
    // Declared here (apply false) so the plugin is on the build classpath without affecting normal builds.
    id("org.checkerframework") apply false
}

var useJacoco = (!project.hasProperty("jacocoEnabled") || project.property("jacocoEnabled").toString().toBoolean())

val nativeClassifier: String = osdetector.classifier

if (useJacoco) {
    apply(plugin = "org.gradle.jacoco")
}

val awsSdkVersion = "2.46.10"
val testcontainersVersion = "1.21.4"
val junitPlatformVersion = "1.14.4"
val junitJupiterVersion = "5.14.4"
val openTelemetryVersion = "1.62.0"

dependencies {

    optionalImplementation("software.amazon.awssdk:rds:$awsSdkVersion")
    optionalImplementation("software.amazon.awssdk:auth:$awsSdkVersion") // Required for IAM (light implementation)
    optionalImplementation("software.amazon.awssdk:http-client-spi:$awsSdkVersion") // Required for IAM (light implementation)
    optionalImplementation("software.amazon.awssdk:sts:$awsSdkVersion")
    optionalImplementation("software.amazon.awssdk:kms:$awsSdkVersion")
    optionalImplementation("software.amazon.awssdk:secretsmanager:$awsSdkVersion")
    // Required only when 'allowAwsLoginSession=true' is used together with a profile that
    // authenticates via a 'login_session' entry. Accessed reflectively so it stays optional.
    optionalImplementation("software.amazon.awssdk:signin:$awsSdkVersion")
    optionalImplementation("com.fasterxml.jackson.core:jackson-databind:2.22.2")
    optionalImplementation("com.zaxxer:HikariCP:4.0.3") // Version 4.+ is compatible with Java 8
    optionalImplementation("com.mchange:c3p0:0.14.2")
    optionalImplementation("org.apache.httpcomponents:httpclient:4.5.14")
    optionalImplementation("org.apache.commons:commons-pool2:2.13.1")
    optionalImplementation("org.jsoup:jsoup:1.23.2")
    optionalImplementation("com.amazonaws:aws-xray-recorder-sdk-core:2.21.1")
    optionalImplementation("io.opentelemetry:opentelemetry-api:$openTelemetryVersion")
    optionalImplementation("io.opentelemetry:opentelemetry-sdk:$openTelemetryVersion")
    optionalImplementation("io.opentelemetry:opentelemetry-sdk-metrics:$openTelemetryVersion")
    optionalImplementation("io.valkey:valkey-glide:2.3.0:$nativeClassifier")
    optionalImplementation("com.github.jsqlparser:jsqlparser:4.9")

    compileOnly("org.checkerframework:checker-qual:3.55.1")
    compileOnly("com.mysql:mysql-connector-j:26.7.0")
    compileOnly("org.postgresql:postgresql:42.7.13")
    compileOnly("org.mariadb.jdbc:mariadb-java-client:3.5.10")
    compileOnly("org.osgi:org.osgi.core:6.0.0")
    compileOnly("org.jetbrains.kotlin:kotlin-stdlib:2.4.20")

    // The following dependency will be included in federated-auth bundle jar.
    federatedAuthBundleImplementation("org.apache.httpcomponents:httpclient:4.5.14")
    federatedAuthBundleImplementation("software.amazon.awssdk:rds:$awsSdkVersion")
    federatedAuthBundleImplementation("software.amazon.awssdk:sts:$awsSdkVersion")
    federatedAuthBundleImplementation("org.jsoup:jsoup:1.23.2")

    testImplementation("org.checkerframework:checker-qual:3.55.1")
    testImplementation("org.junit.platform:junit-platform-commons:$junitPlatformVersion")
    testImplementation("org.junit.platform:junit-platform-engine:$junitPlatformVersion")
    testImplementation("org.junit.platform:junit-platform-launcher:$junitPlatformVersion")
    testImplementation("org.junit.platform:junit-platform-suite-engine:$junitPlatformVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-api:$junitJupiterVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-params:$junitJupiterVersion")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")

    testImplementation("org.apache.commons:commons-dbcp2:2.14.0")
    testImplementation("org.postgresql:postgresql:42.7.13")
    testImplementation("com.mysql:mysql-connector-j:26.7.0")
    testImplementation("org.mariadb.jdbc:mariadb-java-client:3.5.10")
    testImplementation("com.zaxxer:HikariCP:4.0.3") // Version 4.+ is compatible with Java 8
    testImplementation("com.mchange:c3p0:0.14.2")
    testImplementation("org.springframework.boot:spring-boot-starter-jdbc:2.7.13") // 2.7.13 is the last version compatible with Java 8
    testImplementation("org.mockito:mockito-inline:4.11.0") // 4.11.0 is the last version compatible with Java 8
    testImplementation("software.amazon.awssdk:kms:$awsSdkVersion")
    testImplementation("software.amazon.awssdk:rds:$awsSdkVersion", )
    testImplementation("software.amazon.awssdk:auth:$awsSdkVersion") // Required for IAM (light implementation)
    testImplementation("software.amazon.awssdk:http-client-spi:$awsSdkVersion") // Required for IAM (light implementation)
    testImplementation("software.amazon.awssdk:ec2:$awsSdkVersion")
    testImplementation("software.amazon.awssdk:secretsmanager:$awsSdkVersion")
    testImplementation("software.amazon.awssdk:sts:$awsSdkVersion")
    testImplementation("software.amazon.awssdk:signin:$awsSdkVersion")

    // The two Orchestra jars the in-container code reads its environment through, and only those two.
    // orchestra-core and orchestra-instruments are deliberately absent: they target Java 17 while this source
    // set compiles for Java 8, and they provision environments, which is no business of code running inside
    // one. orchestra-contract and orchestra-client-java are Java 8 precisely so they can be used here.
    testImplementation(files("lib/orchestra/orchestra-contract-0.1.0-SNAPSHOT.jar"))
    testImplementation(files("lib/orchestra/orchestra-client-java-0.1.0-SNAPSHOT.jar"))

    // Note: all org.testcontainers dependencies should have the same version
    testImplementation("org.testcontainers:testcontainers:$testcontainersVersion")
    testImplementation("org.testcontainers:mysql:$testcontainersVersion")
    testImplementation("org.testcontainers:postgresql:$testcontainersVersion")
    testImplementation("org.testcontainers:mariadb:$testcontainersVersion")
    testImplementation("org.testcontainers:junit-jupiter:$testcontainersVersion")
    testImplementation("org.apache.poi:poi-ooxml:5.5.1")
    testImplementation("org.slf4j:slf4j-simple:2.0.19")
    testImplementation("com.fasterxml.jackson.core:jackson-databind:2.22.2")
    testImplementation("tools.jackson.core:jackson-databind:3.2.2") // Required for java17 multi-release classes under Java 17+
    testImplementation("com.amazonaws:aws-xray-recorder-sdk-core:2.21.1")
    testImplementation("io.opentelemetry:opentelemetry-api:$openTelemetryVersion")
    testImplementation("io.opentelemetry:opentelemetry-sdk:$openTelemetryVersion")
    testImplementation("io.opentelemetry:opentelemetry-sdk-metrics:$openTelemetryVersion")
    testImplementation("io.opentelemetry:opentelemetry-exporter-otlp:$openTelemetryVersion")
    testImplementation("org.apache.commons:commons-pool2:2.13.1")
    testImplementation("org.jsoup:jsoup:1.23.2")
    testImplementation("de.vandermeer:asciitable:0.3.2")
    testImplementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.22.2")
    testImplementation("io.valkey:valkey-glide:2.3.0:$nativeClassifier") // Note: to run unit tests on ARM Mac, change native classifier to "osx-x86_64"
    testImplementation("com.github.jsqlparser:jsqlparser:4.9") // Required by sqlParser/autoReadWriteSplitting and kmsEncryption plugins
    // XA transaction managers for XADataSource integration tests (both are exercised for compatibility).
    testImplementation("org.jboss.narayana.jta:narayana-jta:5.12.7.Final") // 5.11.x is the last Java 8-compatible line
    // Narayana declares jboss-logging as optional/provided; add it explicitly or jtaLogger fails to init.
    testImplementation("org.jboss.logging:jboss-logging:3.6.3.Final")
    testImplementation("com.atomikos:transactions-jta:6.0.1")
    testImplementation("com.atomikos:transactions-jdbc:6.0.1")
}

repositories {
    mavenCentral()
}

if (useJacoco) {
    tasks.check {
        dependsOn("jacocoTestCoverageVerification")
    }
}

tasks.test {
    filter.excludeTestsMatching("integration.*")
}

java {
    withJavadocJar()
    withSourcesJar()
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(8))
    }
}

// Create a separate source set for Java 11+ specific code (e.g., java.lang.ref.Cleaner)
val java11 = sourceSets.create("java11") {
    java {
        srcDir("src/main/java11")
    }
    // Make java11 source set depend on main source set
    compileClasspath += sourceSets.main.get().output
}

// Configure the java11 source set to compile with Java 11
tasks.named<JavaCompile>(java11.compileJavaTaskName) {
    javaCompiler.set(javaToolchains.compilerFor {
        languageVersion.set(JavaLanguageVersion.of(11))
    })
    options.release.set(11)
    // Ensure main classes are compiled before java11 classes
    dependsOn(tasks.compileJava)
}

// Create a separate source set for Java 17+ specific code (e.g., Jackson 3.x based implementations)
val java17 = sourceSets.create("java17") {
    java {
        srcDir("src/main/java17")
    }
    // Include main output and full compile classpath (provides optionalImplementation deps like awssdk, httpclient, etc.)
    compileClasspath += sourceSets.main.get().output + sourceSets.main.get().compileClasspath
}

// Configure the java17 source set to compile with Java 17
tasks.named<JavaCompile>(java17.compileJavaTaskName) {
    javaCompiler.set(javaToolchains.compilerFor {
        languageVersion.set(JavaLanguageVersion.of(17))
    })
    options.release.set(17)
    // Ensure main classes are compiled before java17 classes
    dependsOn(tasks.compileJava)
}

// Create a separate source set for Java 24+ specific code (e.g., PgTargetDriverDialect)
val java24 = sourceSets.create("java24") {
    java {
        srcDir("src/main/java24")
    }
    // Make java24 source set depend on main source set
    compileClasspath += sourceSets.main.get().output
}

// java24 source set presents an alternative implementation to be executed under java24 environment.
// However java24 source set is compiled with java8.
tasks.named<JavaCompile>(java24.compileJavaTaskName) {
    // Ensure main classes are compiled before java24 classes
    dependsOn(tasks.compileJava)
}

// Hibernate v7.3 requires at least Java 17
// Create a source set for these tests and compile the Hibernate tests against Java 17
val hibernateTest = sourceSets.create("hibernateTest") {
    java {
        srcDir("src/test/java17")
    }
    // Include main output and test output (excluding Hibernate) for dependencies
    val paths = sourceSets.main.get().output + sourceSets.test.get().output + sourceSets.test.get().compileClasspath
    compileClasspath += paths
    runtimeClasspath += paths
}

tasks.named<JavaCompile>(hibernateTest.compileJavaTaskName) {
    javaCompiler.set(javaToolchains.compilerFor {
        languageVersion.set(JavaLanguageVersion.of(17))
    })
    options.release.set(17)
    // Ensure test classes are compiled before Hibernate test classes
    dependsOn(tasks.compileTestJava)
}

// Orchestra targets Java 17, and the `test` source set is compiled for Java 8 because the same classes run
// inside the container against every supported JVM. Putting the Orchestra-based host runner in `test` fails
// with "cannot access software.amazon.orchestra.EnvConfiguration" - a Java 8 compiler cannot read Java 17
// class files.
//
// So it gets its own source set, exactly as hibernateTest does for the same reason. That split is not a
// workaround: this code only ever runs in the outer JVM that provisions environments, which already requires
// a modern JDK, while the in-container code has to stay Java 8 compatible.
val orchestraTest = sourceSets.create("orchestraTest") {
    java {
        srcDir("src/test/orchestra")
    }
    val outputs = sourceSets.main.get().output + sourceSets.test.get().output
    compileClasspath += outputs + sourceSets.test.get().compileClasspath
    // Runtime borrows test's *runtime* classpath, not its compile classpath. The JUnit Jupiter engine is a
    // testRuntimeOnly dependency, so a runtime classpath built from compileClasspath has no engine - and
    // without an engine nothing is discovered, which presents as "No tests found for given includes" rather
    // than as a missing dependency. hibernateTest gets away with the compile-only form because it is only
    // ever compiled, never run as its own task.
    runtimeClasspath += outputs + sourceSets.test.get().runtimeClasspath
}

tasks.named<JavaCompile>(orchestraTest.compileJavaTaskName) {
    javaCompiler.set(javaToolchains.compilerFor {
        languageVersion.set(JavaLanguageVersion.of(17))
    })
    options.release.set(17)
    dependsOn(tasks.compileTestJava)
}

dependencies {
    add(java11.compileOnlyConfigurationName, "org.checkerframework:checker-qual:3.55.1")
    add(java17.compileOnlyConfigurationName, "org.checkerframework:checker-qual:3.55.1")
    add(java17.implementationConfigurationName, "tools.jackson.core:jackson-databind:3.2.2")
    add(java24.compileOnlyConfigurationName, "org.checkerframework:checker-qual:3.55.1")
    // The java24 variant of PgTargetDriverDialect has to declare every method the base variant
    // declares - a multi-release JAR replaces the class wholesale, so an omitted method silently
    // falls back to GenericTargetDriverDialect at runtime. Some of those methods reference pgjdbc
    // types directly, so the driver has to be on this source set's compile classpath too. It stays
    // compileOnly here for the same reason as in the main source set: pgjdbc is an optional
    // dependency supplied by the application.
    add(java24.compileOnlyConfigurationName, "org.postgresql:postgresql:42.7.13")
    // Hibernate test dependencies (Java 17+)
    add(hibernateTest.implementationConfigurationName, "org.hibernate.orm:hibernate-core:7.4.9.Final")
    add(hibernateTest.implementationConfigurationName, "jakarta.persistence:jakarta.persistence-api:3.2.0")

    // Orchestra, the environment-provisioning library replacing this harness's TestEnvironment,
    // TestEnvironmentProvider, AuroraTestUtility and ContainerHelper.
    //
    // Copied jars rather than a Maven coordinate, because Orchestra is not published yet. Each jar's name
    // and manifest carry the version and OrchestraVersion logs it at the start of every run, so a result can
    // still be attributed to a build after the jars are copied. Replace with a normal dependency once
    // Orchestra publishes.
    add(orchestraTest.implementationConfigurationName, fileTree("lib/orchestra") { include("*.jar") })
    // Orchestra's own transitive needs. It uses Testcontainers to provision Docker resources and the AWS
    // SDK to provision RDS, and a fileTree dependency carries no transitives.
    add(orchestraTest.implementationConfigurationName, "org.testcontainers:testcontainers:$testcontainersVersion")
    add(orchestraTest.implementationConfigurationName, "software.amazon.awssdk:rds:$awsSdkVersion")
    add(orchestraTest.implementationConfigurationName, "software.amazon.awssdk:ec2:$awsSdkVersion")
    add(orchestraTest.implementationConfigurationName, "software.amazon.awssdk:sts:$awsSdkVersion")
    add(orchestraTest.implementationConfigurationName, "software.amazon.awssdk:secretsmanager:$awsSdkVersion")
}

fun CopySpec.addMultiReleaseContents() {
    into("META-INF/versions/11") {
        from(java11.output)
    }
    into("META-INF/versions/17") {
        from(java17.output)
    }
    into("META-INF/versions/24") {
        from(java24.output)
    }
}

tasks.named("sourcesJar") {
    dependsOn("preprocessVersion")
}

if (useJacoco) {
    tasks.named("jacocoTestCoverageVerification") {
        dependsOn("preprocessVersion")
        dependsOn("compileJava")
        dependsOn("processResources")
    }
}

checkstyle {
    // Checkstyle versions 7.x, 8.x, and 9.x are supported by JRE version 8 and above.
    toolVersion = "9.3"
    // Fail the build if there is at least one Checkstyle warning.
    maxWarnings = 0
    configDirectory.set(File(rootDir, "config/checkstyle"))
    configFile = configDirectory.get().file("google_checks.xml").asFile

    // Checkstyle will throw an error if a driver-specific import is detected in the new changes.
    // If the change is intentional, add the file to the suppression filter in checkstyle-suppressions.xml.
    configProperties = mapOf("suppressionFile" to configDirectory.get().file("checkstyle-suppressions.xml").asFile)
}

spotless {
    isEnforceCheck = false

    format("misc") {
        target("*.gradle", "*.md", ".gitignore")

        trimTrailingWhitespace()
        indentWithTabs()
        endWithNewline()
    }

    java {
        googleJavaFormat("1.7")
    }
}

spotbugs {
    ignoreFailures.set(true)
}

// ---------------------------------------------------------------------------
// Checker Framework (NullnessChecker, enforcing, whole source tree).
//
// Opt-in via a property, and NOT because the findings are optional - the whole
// software.amazon.jdbc tree is checker-clean and the CI job below enforces that.
// The property exists because this block retargets compileJava:
//   - the checker itself cannot run on JDK 8, so compileJava is forced onto a
//     JDK 17 toolchain with release 11, whereas the shipped artifact must stay
//     Java 8. Enabling this unconditionally would silently change the bytecode
//     target of the published jar.
// So this is a verification build, not a packaging build. Run it with:
//   ./gradlew :aws-advanced-jdbc-wrapper:compileJava -PenableCheckerFramework
// On a JDK 8 host, run it inside a JDK 17 container (scripts/run-checker-framework.sh).
//
// Behaviour:
//   - Only the NullnessChecker is enabled (Optional/Regex add noise for little gain here).
//   - Enforcing: findings are compile errors and fail the build. Do not add -Awarns
//     back; if a finding is a false positive, annotate it or add a narrowly-scoped
//     @SuppressWarnings with a justifying comment (see the existing conventions).
// ---------------------------------------------------------------------------
if (project.hasProperty("enableCheckerFramework")) {
    apply(plugin = "org.checkerframework")

    configure<CheckerFrameworkExtension> {
        checkers = listOf(
            "org.checkerframework.checker.nullness.NullnessChecker"
        )
        // Scope: every named class in the software.amazon.jdbc tree. The incremental
        // adoption is complete, so this list is no longer a subset - each alternative below
        // corresponds to a package that is fully clean, and together they cover the whole
        // main source set.
        //
        // Why the filter is still here rather than deleted: dropping -AonlyDefs also pulls
        // ANONYMOUS classes into scope, and the codebase widely uses the double-brace
        // initializer idiom (`new HashMap<K, V>() {{ put(...); }}`) for enum mappings and
        // plugin/dialect registries. The checker rejects those `put`/`add` calls as
        // invocations on an @UnderInitialization receiver, which produces 100+ findings
        // across ~25 files. Covering anonymous classes therefore requires first replacing
        // that idiom (e.g. with static factory helpers) - tracked as follow-up work, kept
        // separate from enabling enforcement so this change stays reviewable.
        //
        // When adding a new package, add it here too, otherwise it silently escapes the gate.
        //
        // No end-anchor: matching an outer class also covers its nested classes (and, for
        // "pkg\.\w+", the classes of nested sub-packages such as util.telemetry.*). The
        // "plugin\.[A-Z]\w*" entry matches only classes directly in the plugin package
        // (upper-case class names), not its lower-case sub-packages (efm, cache, ...).
        extraJavacArgs = listOf(
            "-AonlyDefs=^software\\.amazon\\.jdbc\\.(ConnectionPluginManager|PluginServiceImpl"
                + "|wrapper\\.\\w+"
                + "|util\\.\\w+"
                + "|exceptions\\.\\w+|cleanup\\.\\w+|authentication\\.\\w+"
                + "|hostavailability\\.\\w+|osgi\\.\\w+|profile\\.\\w+"
                + "|states\\.\\w+|ds\\.\\w+"
                + "|targetdriverdialect\\.\\w+|dialect\\.\\w+"
                + "|hostlistprovider\\.\\w+"
                + "|plugin\\.[A-Z]\\w*"
                + "|plugin\\.cache\\.\\w+|plugin\\.bluegreen\\.\\w+"
                + "|plugin\\.failover\\.\\w+|plugin\\.gdbfailover\\.\\w+"
                + "|plugin\\.efm\\.\\w+|plugin\\.federatedauth\\.\\w+"
                + "|plugin\\.iam\\.\\w+|plugin\\.limitless\\.\\w+"
                + "|plugin\\.customendpoint\\.\\w+|plugin\\.strategy\\.\\w+"
                + "|plugin\\.dev\\.\\w+|plugin\\.staledns\\.\\w+"
                + "|plugin\\.sqlparser\\.\\w+"
                + "|parser\\.\\w+|plugin\\.failover2\\.\\w+"
                + "|plugin\\.readwritesplitting\\.\\w+|plugin\\.encryption\\.\\w+"
                + "|[A-Z]\\w*)",
            // Keep the output focused and avoid drowning in framework boilerplate.
            "-AsuppressWarnings=uninitialized",
            // Enforcing mode caps errors at 100 by default; raise it so a regression shows
            // its full extent in one run instead of being truncated.
            "-Xmaxerrs", "10000"
        )
    }

    // The checker must run on JDK 11+. Force this source set's compiler to JDK 17.
    tasks.named<JavaCompile>("compileJava") {
        javaCompiler.set(javaToolchains.compilerFor {
            languageVersion.set(JavaLanguageVersion.of(17))
        })
        // Compile against release 11 (main source is Java-8 compatible). Measurement-only build.
        options.release.set(11)

        // The Checker Framework plugin adds the error-prone `javac` jar via
        // -Xbootclasspath/p as a JDK-8 compatibility shim. That option is unsupported
        // on JDK 17 and crashes the compiler worker. Strip that entry and instead grant
        // the module access the checker needs to read jdk.compiler internals on JDK 17
        // (the standard --add-exports/--add-opens set from the Checker Framework manual).
        doFirst {
            val moduleArgs = listOf(
                "--add-exports=jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED",
                "--add-exports=jdk.compiler/com.sun.tools.javac.code=ALL-UNNAMED",
                "--add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED",
                "--add-exports=jdk.compiler/com.sun.tools.javac.main=ALL-UNNAMED",
                "--add-exports=jdk.compiler/com.sun.tools.javac.model=ALL-UNNAMED",
                "--add-exports=jdk.compiler/com.sun.tools.javac.parser=ALL-UNNAMED",
                "--add-exports=jdk.compiler/com.sun.tools.javac.processing=ALL-UNNAMED",
                "--add-exports=jdk.compiler/com.sun.tools.javac.tree=ALL-UNNAMED",
                "--add-exports=jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED",
                "--add-opens=jdk.compiler/com.sun.tools.javac.comp=ALL-UNNAMED"
            )
            val existing = (options.forkOptions.jvmArgs ?: emptyList())
                .filterNot { it.startsWith("-Xbootclasspath/p") }
            options.forkOptions.jvmArgs = (existing + moduleArgs).distinct()
        }
    }
}

tasks.spotbugsMain {
    reports.create("html") {
        required.set(true)
        outputLocation.set(file("${layout.buildDirectory.get()}/reports/spotbugsMain.html"))
        setStylesheet("fancy-hist.xsl")
    }
}
tasks.spotbugsTest {
    reports.create("html") {
        required.set(true)
        outputLocation.set(file("${layout.buildDirectory.get()}/reports/spotbugsTest.html"))
        setStylesheet("fancy-hist.xsl")
    }
}

if (useJacoco) {
    tasks.withType<JacocoCoverageVerification> {
        violationRules {
            rule {
                limit {
                    minimum = BigDecimal(0.30)
                }
            }
        }

        afterEvaluate {
            classDirectories.setFrom(files(classDirectories.files.map {
                fileTree(it).apply {
                    exclude(
                        "software/amazon/jdbc/wrapper/*",
                        "software/amazon/jdbc/util/*",
                        "software/amazon/jdbc/profile/*",
                        "software/amazon/jdbc/plugin/cache/DataLocalCacheConnectionPlugin*"
                    )
                }
            }))
        }
    }

    tasks.withType<JacocoReport> {
        afterEvaluate {
            classDirectories.setFrom(files(classDirectories.files.map {
                fileTree(it).apply {
                    exclude(
                        "software/amazon/jdbc/wrapper/*",
                        "software/amazon/jdbc/util/*",
                        "software/amazon/jdbc/profile/*",
                        "software/amazon/jdbc/plugin/cache/DataLocalCacheConnectionPlugin*"
                    )
                }
            }))
        }
    }
}

tasks.jar {
    dependsOn(tasks.named(java11.compileJavaTaskName))
    dependsOn(tasks.named(java17.compileJavaTaskName))
    dependsOn(tasks.named(java24.compileJavaTaskName))

    from("${project.rootDir}") {
        include("README")
        include("LICENSE")
        include("THIRD-PARTY-LICENSES")
        into("META-INF/")
    }

    from("${layout.buildDirectory.get()}/META-INF/services/") {
        into("META-INF/services/")
    }

    bundle {
        bnd(
            """
            -exportcontents: software.*
            -removeheaders: Created-By
            -noclassforname: true
            -noextraheaders: true
            Bundle-Description: Amazon Web Services (AWS) Advanced JDBC Wrapper Driver
            Bundle-DocURL: https://github.com/aws/aws-advanced-jdbc-wrapper
            Bundle-Vendor: Amazon Web Services (AWS)
            Import-Package: javax.sql, javax.transaction.xa, javax.naming, javax.security.sasl;resolution:=optional, *;resolution:=optional
            Bundle-Activator: software.amazon.jdbc.osgi.WrapperBundleActivator
            Bundle-SymbolicName: software.aws.rds
            Bundle-Name: Amazon Web Services (AWS) Advanced JDBC Wrapper Driver
            Bundle-Copyright: Copyright Amazon.com Inc. or affiliates.
            Require-Capability: osgi.ee;filter:="(&(|(osgi.ee=J2SE)(osgi.ee=JavaSE))(version>=1.8))"
            """
        )
    }

    doFirst {
        mkdir("${layout.buildDirectory.get()}/META-INF/services/")
        val driverFile = File("${layout.buildDirectory.get()}/META-INF/services/java.sql.Driver")
        if (driverFile.createNewFile()) {
            driverFile.writeText("software.amazon.jdbc.Driver")
        }
    }

    manifest {
        attributes["Multi-Release"] = "true"
    }

    // Add multi-release content after bnd processing
    doLast {
        val java11Dir = java11.output.classesDirs.files.first()
        if (java11Dir.exists()) {
            ant.withGroovyBuilder {
                "jar"("destfile" to archiveFile.get().asFile, "update" to true) {
                    "zipfileset"("dir" to java11Dir, "prefix" to "META-INF/versions/11")
                }
            }
        }
        val java17Dir = java17.output.classesDirs.files.first()
        if (java17Dir.exists()) {
            ant.withGroovyBuilder {
                "jar"("destfile" to archiveFile.get().asFile, "update" to true) {
                    "zipfileset"("dir" to java17Dir, "prefix" to "META-INF/versions/17")
                }
            }
        }
        val java24Dir = java24.output.classesDirs.files.first()
        if (java24Dir.exists()) {
            ant.withGroovyBuilder {
                "jar"("destfile" to archiveFile.get().asFile, "update" to true) {
                    "zipfileset"("dir" to java24Dir, "prefix" to "META-INF/versions/24")
                }
            }
        }
    }
}

configurations {
    federatedAuthBundleImplementation {
        isCanBeResolved = true
    }
}

tasks.shadowJar {

    dependsOn(tasks.named(java11.compileJavaTaskName))
    dependsOn(tasks.named(java17.compileJavaTaskName))
    dependsOn(tasks.named(java24.compileJavaTaskName))

    configurations = listOf(project.configurations.federatedAuthBundleImplementation.get())
    from(sourceSets.main.get().output)

    archiveBaseName.set("aws-advanced-jdbc-wrapper")
    archiveClassifier.set("bundle-federated-auth")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE

    from("${layout.buildDirectory.get()}/META-INF/services/") {
        into("META-INF/services/")
    }

    mergeServiceFiles()

    manifest {
        attributes("Implementation-Title" to "AWS Advanced JDBC Wrapper")
        attributes("Implementation-Version" to project.version)
        attributes("Implementation-Vendor" to "Amazon Web Services")
        attributes("Export-Package" to
                "software.amazon.jdbc.*,shaded.software.amazon.awssdk.*,shaded.org.apache.http.*,shaded.org.jsoup.*")
        attributes["Multi-Release"] = "true"
    }

    relocate("au", "shaded.au")
    relocate("com", "shaded.com")
    relocate("io", "shaded.io")
    relocate("org", "shaded.org")
    relocate("META-INF/versions/*/org", "shaded.org")

    relocate("software", "shaded.software") {
        exclude("software.amazon.jdbc.**")
    }

    // Add multi-release content after bnd processing
    doLast {
        val java11Dir = java11.output.classesDirs.files.first()
        if (java11Dir.exists()) {
            ant.withGroovyBuilder {
                "jar"("destfile" to archiveFile.get().asFile, "update" to true) {
                    "zipfileset"("dir" to java11Dir, "prefix" to "META-INF/versions/11")
                }
            }
        }
        val java17Dir = java17.output.classesDirs.files.first()
        if (java17Dir.exists()) {
            ant.withGroovyBuilder {
                "jar"("destfile" to archiveFile.get().asFile, "update" to true) {
                    "zipfileset"("dir" to java17Dir, "prefix" to "META-INF/versions/17")
                }
            }
        }
        val java24Dir = java24.output.classesDirs.files.first()
        if (java24Dir.exists()) {
            ant.withGroovyBuilder {
                "jar"("destfile" to archiveFile.get().asFile, "update" to true) {
                    "zipfileset"("dir" to java24Dir, "prefix" to "META-INF/versions/24")
                }
            }
        }
    }

}

junitHtmlReport {
    // The maximum depth to traverse from the results dir.
    // Any eligible reports will be included
    maxDepth = 9

    //RAG status css overrides
    cssRed = "red"
    cssAmber = "orange"
    cssGreen = "green"

    //Processing directories
    testResultsDir = "test-results"
    testReportsDir = "report"

    //Fail build when no XML files to process
    isFailOnEmpty = false
}

tasks.withType<Test> {
    dependsOn("jar")
    dependsOn(tasks.named(hibernateTest.compileJavaTaskName))
    testLogging {
        this.showStandardStreams = true
    }
    useJUnitPlatform()
    outputs.upToDateWhen { false }

    // Include hibernate test classes in the test classpath
    testClassesDirs += hibernateTest.output.classesDirs
    classpath += hibernateTest.output

    // No blanket property forwarding. The retired harness selected environments with test-no-* properties,
    // so this block forwarded anything with that prefix to every Test task; Orchestra tasks declare what
    // they accept, and forward exactly that, which is what makes an unsupported request visible instead of
    // silently dropped.

    // Disable the test report for the individual test task
    reports.junitXml.required.set(true)
    reports.html.required.set(false)

    systemProperty("java.util.logging.config.file", "${project.layout.buildDirectory.get()}/resources/test/logging-test.properties")

    if (!name.contains("performance")) {
        finalizedBy("junitHtmlReport")
    }

    val testReportsPath = "${layout.buildDirectory.get()}/test-results"
    val testReportsDir: File = file(testReportsPath)
    doFirst {
        testReportsDir.deleteRecursively()
    }
}

tasks.register("maskJunitHtmlReport") {
    doLast {
        if (project.file("${layout.buildDirectory.get()}/report/data.js").exists()) {
            val jsFile = project.file("${layout.buildDirectory.get()}/report/data.js")
            val text = jsFile.readText()
            val regex = "\"([^\"]*(AWS_ACCESS_|AWS_SECRET_|AWS_SESSION_)[^\"]*)\", value: \"([^\"]*)\"".toRegex(setOf(RegexOption.MULTILINE, RegexOption.IGNORE_CASE))
            val maskedText = regex.replace(text, "\"$1\", value: \"*****\"")
            jsFile.writeText(maskedText)
        }
    }
}

// The migrated equivalent of test-all-pg-aurora, provisioned by Orchestra.
//
// A separate task rather than a replacement, so the existing harness stays runnable while the migration is
// proven. It selects nothing with test-no-* properties: the composition is declared in OrchestraTestRunner,
// which is the difference the migration is about.
tasks.register<Test>("orchestra-test-pg-aurora") {
    group = "verification"
    description = "Runs the in-container suite against an Orchestra-provisioned Aurora PostgreSQL cluster."

    testClassesDirs = orchestraTest.output.classesDirs
    classpath = orchestraTest.runtimeClasspath

    // Name the exact driver jar to give the container, and depend on it so it exists.
    //
    // Replaces copying all of build/libs, which accumulates. A real checkout had six jars there - three
    // driver versions plus sources, javadoc and a shaded federated-auth bundle - and the in-container build
    // globs libs/*.jar, so every one of them landed on the test classpath. The bundle's module-info requires
    // commons.math3, which is absent, so the test JVM died during boot layer initialisation: no tests, no
    // JUnit XML, just "Gradle Test Executor 1 finished with non-zero exit value 1". It was intermittent
    // too, since it depended on what previous builds had left behind.
    val driverJar = tasks.named<Jar>("jar")
    dependsOn(driverJar)
    systemProperty("orchestra-driver-jar", driverJar.get().archiveFile.get().asFile.absolutePath)

    // Java 17, not the Java 8 launcher the other test tasks use. Orchestra's classes are version 61 and a
    // Java 8 JVM refuses to load them: "has been compiled by a more recent version of the Java Runtime".
    // This task only provisions - the suite it launches still runs on whatever JVM the container has, which
    // is what the target-JVM axis varies.
    javaLauncher.set(javaToolchains.launcherFor {
        languageVersion.set(JavaLanguageVersion.of(17))
    })

    // Only the runner. Everything else in this source set is an instrument or a configuration object, and
    // JUnit's scanning tries to execute them as test classes otherwise.
    filter.includeTestsMatching("integration.orchestra.OrchestraTestRunner.runTests")

    useJUnitPlatform()
    // Never up to date: whether an AWS call still behaves the way an instrument assumes is not a function
    // of the source tree, which is all Gradle's staleness check can see.
    outputs.upToDateWhen { false }

    // The suite runs inside a container and provisions a real cluster, so the run is long and its output is
    // the only progress indicator.
    testLogging {
        this.showStandardStreams = true
    }

    // Forward the suite-selection properties from the Gradle JVM into this task's JVM.
    //
    // Without this they never arrive. OrchestraAuroraConfig reads them with System.getProperty in order to
    // pass them on to the in-container Gradle, but it runs in the forked test JVM, which does not inherit
    // Gradle's own -D options - so every filter passed on the command line was silently ignored and the full
    // suite ran regardless. Silently, because an ignored filter looks like a slow run rather than an error.
    doFirst {
        listOf(
            "test-include-tags",
            "test-exclude-tags",
            "test-shard-index",
            "test-shard-count",
            "test-classes",
            // The matrix axes. Omitting them here is exactly the failure this block was written for: the
            // run reported "running 1 composition(s)" and provisioned the default slice, so a matrix
            // request looked like it had been honoured when it had been dropped.
            "orchestra-engines",
            "orchestra-instances",
            "orchestra-bluegreen",
            "orchestra-deployment",
            // Which JDBC driver(s) the in-container suite connects with, which is a different axis from the
            // engine: the MariaDB driver runs against a MySQL engine, and CI pins it that way in
            // test-bgd-mysql-aurora-mariadb-driver and test-bgd-mysql-rds-instance-mariadb-driver.
            "orchestra-drivers",
            // Which JVM(s) the suite runs on, named by TargetJvm. It works by overriding the container
            // image, which is also where the published shape reads the JVM from.
            "orchestra-jvms",
            // Whether to bind the host's ~/.aws into the test container instead of copying resolved keys.
            // Off by default: CI credentials last six hours, longer than a run, and there is no credentials
            // file there to bind. A developer's session token lasts one hour and is refreshed on a timer, so
            // a long local run needs the file rather than a snapshot of it.
            "orchestra-aws-credentials-bind",

            // Which suite to run: the harness's *_ONLY flags and PERFORMANCE, as one choice. It narrows the
            // run to a single test class rather than adding to it, which is why it is a mode.
            "orchestra-suite",

            // The KMS key the encryption suite needs, for a local run that would rather pass it here than
            // export KMS_KEY_ID. CI sets the variable, so it needs neither.
            "orchestra-kms-key",

            // How many times the performance suites repeat each measurement, overriding their built-in
            // defaults. Unset leaves those defaults alone, which is what a real measurement wants; a run that
            // only needs to know the suite works wants one pass.
            "orchestra-repeat-times",

            // Whether to provision telemetry backends. On by default, as in the harness; none disables both.
            "orchestra-telemetry",

            // The regions of an Aurora global database beyond the primary, and how large each one is. Only
            // read when orchestra-deployment=aurora-global; forwarded unconditionally because a property the
            // runner cannot see is indistinguishable from one that was never passed, and each of these costs
            // a region's worth of provisioning time to get wrong.
            "orchestra-secondary-regions",
            "orchestra-secondary-instances"
        ).forEach { name ->
            System.getProperty(name)?.let { systemProperty(name, it) }
        }
    }

    // The in-container build needs the wrapper jars and the compiled test classes, both of which the
    // configuration copies into the container from build output.
    dependsOn("jar", tasks.compileTestJava)
}

// The migrated equivalent of test-all-docker, provisioned by Orchestra and needing no AWS account.
//
// The first step in retiring Toxiproxy rather than working around it: Toxiproxy's last user is the legacy
// harness, and the harness cannot go until every environment kind it provides exists here. This is the
// cheapest of those to prove - a container provisions in under a minute, where a cluster costs an hour.
tasks.register<Test>("orchestra-test-docker") {
    group = "verification"
    description = "Runs the in-container suite against Orchestra-provisioned database containers."

    testClassesDirs = orchestraTest.output.classesDirs
    classpath = orchestraTest.runtimeClasspath

    val driverJar = tasks.named<Jar>("jar")
    dependsOn(driverJar)
    systemProperty("orchestra-driver-jar", driverJar.get().archiveFile.get().asFile.absolutePath)

    // Java 17 for the same reason as the Aurora task: Orchestra's classes are version 61.
    javaLauncher.set(javaToolchains.launcherFor {
        languageVersion.set(JavaLanguageVersion.of(17))
    })

    filter.includeTestsMatching("integration.orchestra.OrchestraDockerRunner.runTests")

    useJUnitPlatform()
    outputs.upToDateWhen { false }

    testLogging {
        this.showStandardStreams = true
    }

    listOf(
        "test-include-tags",
        "test-exclude-tags",
        "test-shard-index",
        "test-shard-count",
        "test-classes",
        // The JVM axis applies to Docker environments too, and this is the cheapest task to exercise it in.
        "orchestra-jvms",
        // Which database server to run: pg or mysql. The harness's Docker matrix varies the server, and that
        // matrix is the PR gate, so this axis is what makes this task a replacement for it rather than a
        // narrower version of it.
        "orchestra-engines",

        // Whether to provision the four Valkey caches and run the caching tests instead of the rest of the
        // suite. The harness splits these into two jobs over the same matrix - test-all-docker excludes the
        // caching tag, test-all-caching includes it - and this is that split.
        "orchestra-caching",
        // Telemetry applies here too, and is on by default. Forwarded so that asking for none is honoured
        // rather than dropped: the runner reads this forked JVM's properties, not the Gradle command line, so
        // a property missing from this list is invisible to it.
        "orchestra-telemetry",
        // Not because a suite runs here - none does - but so that asking for one is rejected rather than
        // silently ignored, which is what an unforwarded property would produce.
        "orchestra-suite"
    ).forEach { name ->
        System.getProperty(name)?.let { systemProperty(name, it) }
    }
}

// The migrated equivalent of test-hibernate-only, provisioned by Orchestra and needing no AWS account.
//
// A separate task because it is a separate composition: nothing of this repository's suite runs. The container
// holds a pinned Hibernate ORM checkout, the wrapper is on its driver path, and what executes is Hibernate's
// own test suite against a Postgis database. See OrchestraHibernateRunner.
//
// Hours rather than minutes, and the first run on a machine pays for an image build and Hibernate's entire
// dependency graph on top of that.
//
// Run one Orchestra task at a time in a checkout. Every Test task here clears build/test-results before it
// runs, and the orchestra tasks bind that directory into their containers, so starting a second task pulls the
// results directory out from under the first. That cost a 43-minute Aurora run whose test had already passed:
// its in-container build died on a missing output.bin.idx. This task writes its own archives elsewhere, which
// removes half of the collision; the other half is a property of the shared build directory.
tasks.register<Test>("orchestra-test-hibernate") {
    group = "verification"
    description = "Runs Hibernate ORM's own test suite against Postgis, with the wrapper as its JDBC driver."

    testClassesDirs = orchestraTest.output.classesDirs
    classpath = orchestraTest.runtimeClasspath

    // The jar Hibernate's build will use as its driver. Named by the build rather than globbed, for the reason
    // the other orchestra tasks give: build/libs accumulates, and a shaded bundle on the classpath kills the
    // JVM during boot layer initialisation.
    val driverJar = tasks.named<Jar>("jar")
    dependsOn(driverJar)
    systemProperty("orchestra-driver-jar", driverJar.get().archiveFile.get().asFile.absolutePath)

    // Java 17 to run Orchestra itself, as the other orchestra tasks do. Unrelated to the JVM inside the
    // container, which is what Hibernate's suite runs on and which must be 17 or later for Hibernate 7.3.
    javaLauncher.set(javaToolchains.launcherFor {
        languageVersion.set(JavaLanguageVersion.of(17))
    })

    filter.includeTestsMatching("integration.orchestra.OrchestraHibernateRunner.runTests")

    useJUnitPlatform()
    outputs.upToDateWhen { false }

    // The only progress indicator for a run measured in hours.
    testLogging {
        this.showStandardStreams = true
    }

    // No test-selection properties. They select tests in our suite, and none of it runs here; the runner's
    // configuration returns an empty set for exactly that reason.
    dependsOn("jar")
}
