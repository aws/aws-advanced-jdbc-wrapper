/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

import com.github.vlsi.gradle.dsl.configureEach
import org.jdbcProxyDriver.buildtools.JavaCommentPreprocessorTask

plugins {
    java
    id("com.github.vlsi.gradle-extensions")
    id("com.github.vlsi.stage-vote-release")
    id("com.github.vlsi.ide")
}

val String.v: String get() = rootProject.extra["$this.version"] as String
val buildVersion = "jdbc-proxy-driver".v + releaseParams.snapshotSuffix

allprojects {
    group = "software.aws.rds"
    version = buildVersion

    repositories {
        mavenCentral()
    }

    tasks {
        configureEach<JavaCommentPreprocessorTask> {
            val re = Regex("^(\\d+)\\.(\\d+)(?:\\.(\\d+))?.*")

            val version = project.version.toString()
            val matchResult = re.find(version)
                ?: throw GradleException("Unable to parse major.minor.patch version parts from project.version '$version'")
            val (major, minor, patch) = matchResult.destructured

            variables.apply {
                put("version", version)
                put("version.major", major)
                put("version.minor", minor)
                put("version.patch", patch.ifBlank { "0" })
            }
        }
    }
}
