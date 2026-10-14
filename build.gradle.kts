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

import com.github.vlsi.gradle.dsl.configureEach
import com.github.vlsi.gradle.properties.dsl.props
import software.amazon.jdbc.buildtools.JavaCommentPreprocessorTask
// Imported rather than fully qualified at the use site: inside a Kotlin DSL build script `java`
// resolves to the `java` plugin extension, so `java.net.*` / `java.util.*` fail to compile.
import java.net.HttpURLConnection
import java.net.URL
import java.util.Base64

plugins {
    java
    publishing
    signing
    id("com.github.vlsi.gradle-extensions")
    id("com.github.vlsi.stage-vote-release")
    id("com.github.vlsi.ide")
}

releaseParams {
    componentName.set("aws-advanced-jdbc-wrapper")
}

val versionMajor = project.property("aws-advanced-jdbc-wrapper.version.major")
val versionMinor = project.property("aws-advanced-jdbc-wrapper.version.minor")
val versionSubminor = Integer.parseInt(
    project.property("aws-advanced-jdbc-wrapper.version.subminor").toString()
) + if (project.property("snapshot") == "true") 1 else 0
val buildVersion =
    "$versionMajor.$versionMinor.$versionSubminor" + if (project.property("snapshot") == "true") "-SNAPSHOT" else ""

allprojects {
    group = "software.amazon.jdbc"
    version = buildVersion

    repositories {
        mavenCentral()
    }

    apply(plugin = "java")
    apply(plugin = "signing")
    apply(plugin = "maven-publish")
    apply(plugin = "com.github.vlsi.ide")

    tasks {
        configureEach<JavaCommentPreprocessorTask> {
            val re = Regex("^(\\d+)\\.(\\d+)(?:\\.(\\d+))?.*")

            val version = project.version.toString()
            val matchResult = re.find(version)
                ?: throw GradleException("Unable to parse major.minor.patch version parts from project.version '$version'")
            val (major, minor, patch) = matchResult.destructured

            val git = org.ajoberstar.grgit.Grgit.open(mapOf("currentDir" to project.rootDir))
            val revision = git.head().id

            variables.apply {
                put("version", version)
                put("version.major", major)
                put("version.minor", minor)
                put("version.patch", patch.ifBlank { "0" })
                put("version.revision", revision.toString())
            }
        }
    }

    val preprocessVersion by tasks.registering(JavaCommentPreprocessorTask::class) {
        baseDir.set(projectDir)
        sourceFolders.add("src/main/version/")
    }

    ide {
        generatedJavaSources(
            preprocessVersion,
            preprocessVersion.get().outputDirectory.get().asFile,
            sourceSets.main
        )
    }

    java {
        registerFeature("optional") {
            usingSourceSet(sourceSets.getByName("main"))
        }
        registerFeature("federatedAuthBundle") {
            usingSourceSet(sourceSets.getByName("main"))
            disablePublication()
        }
    }

    publishing {
        publications {
            if (project.props.bool("nexus.publish", default = false)) {
                create<MavenPublication>(project.name) {
                    groupId = "software.amazon.jdbc"
                    artifactId = "aws-advanced-jdbc-wrapper"
                    version = buildVersion

                    from(components["java"])
                    suppressAllPomMetadataWarnings()

                    pom {
                        name.set("AWS Advanced JDBC Wrapper")
                        description.set(
                            project.description ?: "Amazon Web Services (AWS) Advanced JDBC Wrapper"
                        )
                        url.set("https://github.com/aws/aws-advanced-jdbc-wrapper")
                        licenses {
                            license {
                                name.set("Apache 2.0")
                                url.set("https://www.apache.org/licenses/LICENSE-2.0")
                            }
                        }
                        developers {
                            developer {
                                id.set("amazonwebservices")
                                organization.set("Amazon Web Services")
                                organizationUrl.set("https://aws.amazon.com")
                                email.set("aws-rds-oss@amazon.com")
                            }
                        }
                        scm {
                            connection.set("scm:git:https://github.com/aws/aws-advanced-jdbc-wrapper.git")
                            developerConnection.set("scm:git@github.com:aws/aws-advanced-jdbc-wrapper.git")
                            url.set("https://github.com/aws/aws-advanced-jdbc-wrapper")
                        }
                        issueManagement {
                            system.set("GitHub issues")
                            url.set("https://github.com/aws/aws-advanced-jdbc-wrapper/issues")
                        }
                    }
                }

                signing {
                    if (project.hasProperty("signing.keyId")
                        && project.property("signing.keyId") != ""
                        && project.hasProperty("signing.password")
                        && project.property("signing.password") != ""
                        && project.hasProperty("signing.secretKeyRingFile")
                        && project.property("signing.secretKeyRingFile") != ""
                    ) {
                        sign(publishing.publications[project.name])
                    }
                }
            }
        }
        repositories {
            maven {
                url = if (project.property("snapshot") == "true") {
                    uri("https://central.sonatype.com/repository/maven-snapshots/")
                } else {
                    uri("https://ossrh-staging-api.central.sonatype.com/service/local/staging/deploy/maven2/")
                }
                credentials {
                    username = System.getenv("MAVEN_USERNAME")
                    password = System.getenv("MAVEN_PASSWORD")
                }
            }

            mavenLocal()
        }
    }
}

/**
 * Transfers the staging repository created by the preceding upload to the Central Publisher Portal.
 *
 * This deliberately does NOT use `POST /manual/upload/defaultRepository/<namespace>`. That endpoint
 * resolves a namespace to "the" default staging repository, and once a nested namespace was also
 * registered on the same Sonatype account (`software.amazon.orchestra` alongside `software.amazon`)
 * it resolved the parent to the child and answered
 * `No repository found for .../software.amazon.orchestra--default-repository`. Looking the repository
 * up by key removes that ambiguity. See
 * https://central.sonatype.org/publish/publish-portal-ossrh-staging-api/ ("Make Separate Requests").
 *
 * Two constraints this task depends on:
 *  - The OSSRH Staging API isolates staging repositories by the uploading client's IP, so this must
 *    run in the same CI job as the upload.
 *  - Every HTTP call is status-checked. The previous implementation was a bare `curl` with no
 *    `--fail`, so an API error printed under `BUILD SUCCESSFUL` and a release silently published
 *    nothing to Maven Central.
 */
tasks.register("postPublishToMaven") {
    group = "publishing"
    description = "Transfers the staged release upload to the Central Publisher Portal."

    doLast {
        val namespace = "software.amazon"
        val apiBase = "https://ossrh-staging-api.central.sonatype.com"

        val username = System.getenv("MAVEN_USERNAME")
            ?: throw GradleException("MAVEN_USERNAME is not set; cannot promote the staging repository.")
        val password = System.getenv("MAVEN_PASSWORD")
            ?: throw GradleException("MAVEN_PASSWORD is not set; cannot promote the staging repository.")
        val authorization = "Bearer " + Base64.getEncoder()
            .encodeToString("$username:$password".toByteArray(Charsets.UTF_8))

        fun call(method: String, url: String): String {
            val connection = URL(url).openConnection() as HttpURLConnection
            try {
                connection.requestMethod = method
                connection.setRequestProperty("Authorization", authorization)
                connection.setRequestProperty("Accept", "application/json")
                connection.connectTimeout = 30_000
                connection.readTimeout = 120_000
                if (method == "POST") {
                    connection.doOutput = true
                    connection.setRequestProperty("Content-Type", "application/json")
                    connection.outputStream.use { it.write("{}".toByteArray(Charsets.UTF_8)) }
                }
                val status = connection.responseCode
                val body = (if (status in 200..299) connection.inputStream else connection.errorStream)
                    ?.bufferedReader()?.use { it.readText() } ?: ""
                if (status !in 200..299) {
                    throw GradleException("$method $url failed with HTTP $status: $body")
                }
                return body
            } finally {
                connection.disconnect()
            }
        }

        // Try the namespace-filtered search first, then an unfiltered one. Both leave `ip` at its
        // default of `client`, so even the unfiltered query only returns repositories created from
        // this runner - that is, by the upload that just ran in this job. The fallback exists
        // because namespace handling on this service has proven unreliable: a request about
        // `software.amazon` resolved to the registered child namespace `software.amazon.orchestra`.
        val searches = listOf(
            "$apiBase/manual/search/repositories?profile_id=$namespace",
            "$apiBase/manual/search/repositories"
        )
        var response = ""
        var keys = emptyList<String>()
        for (search in searches) {
            response = call("GET", search)
            keys = Regex("\"key\"\\s*:\\s*\"([^\"]+)\"")
                .findAll(response)
                .map { it.groupValues[1] }
                .toList()
            if (keys.isNotEmpty()) {
                break
            }
            logger.lifecycle("No staging repository returned by {} - response: {}", search, response)
        }

        if (keys.isEmpty()) {
            throw GradleException(
                "No staging repository found after the upload, with or without a namespace filter. "
                    + "The upload step did not leave one behind, so there is nothing to transfer to "
                    + "the Central Publisher Portal and this release would otherwise publish "
                    + "nothing. Last response: $response"
            )
        }
        if (keys.size > 1) {
            logger.lifecycle("Found {} staging repositories for '{}': {}", keys.size, namespace, keys)
        }
        keys.forEach { key ->
            logger.lifecycle("Promoting staging repository '{}' to the Central Publisher Portal.", key)
            call("POST", "$apiBase/manual/upload/repository/$key")
        }
        logger.lifecycle(
            "Promoted {} staging repository/repositories. Verify at "
                + "https://central.sonatype.com/publishing/deployments",
            keys.size
        )
    }
}
