plugins {
    id("java")
    id("org.graalvm.buildtools.native") version "0.10.3"
}

group = "software.amazon.jdbc"
version = "2.5.2"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(project(":aws-advanced-jdbc-wrapper"))
    // TODO: does graalvm only have reachability data for 42.7.3, or for all versions past 42.3.4?
    // https://github.com/oracle/graalvm-reachability-metadata/tree/master/metadata/org.postgresql/postgresql/42.7.3
    // https://www.graalvm.org/native-image/libraries-and-frameworks/#footnote-1
    testImplementation("org.postgresql:postgresql:42.7.3")
    testImplementation("software.amazon.awssdk:rds:2.29.21")
    testImplementation("software.amazon.awssdk:secretsmanager:2.28.11")
    testImplementation("com.fasterxml.jackson.core:jackson-databind:2.17.1")
    testImplementation("org.junit.jupiter:junit-jupiter-params:5.11.3")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.11.3")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.11.3")
}

tasks.test {
    useJUnitPlatform()
}

graalvmNative {
    metadataRepository {
        enabled.set(true)
    }

    agent {
        enabled.set(true)
        defaultMode.set("conditional")
        modes {
            conditional {
                userCodeFilterPath.set("user-code-filter.json")
            }
        }

        metadataCopy {
            inputTaskNames.add("test")
            outputDirectories.add("metadata-output")
        }
    }
}

tasks.register<Exec>("buildNativeImage") {
    dependsOn("build")

    doLast {
        val nativeImageCommand = listOf(
            "native-image",
            "--test-runner",
            "--no-fallback",
            "-cp", sourceSets["main"].runtimeClasspath.asPath,
            "software.amazon.jdbc.ReachabilityTest" // Replace with your test class or entry point
        )

        exec {
            commandLine(nativeImageCommand)
        }
    }
}
