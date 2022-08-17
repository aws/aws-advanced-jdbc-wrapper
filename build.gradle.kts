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
import software.amazon.jdbc.buildtools.JavaCommentPreprocessorTask
import com.github.vlsi.gradle.publishing.dsl.simplifyXml

plugins {
    java
    publishing
    signing
    id("com.github.vlsi.gradle-extensions")
    id("com.github.vlsi.stage-vote-release")
    id("com.github.vlsi.ide")
}

val versionMajor = project.property("aws-advanced-jdbc-wrapper.version.major")
val versionMinor = project.property("aws-advanced-jdbc-wrapper.version.minor")
val versionSubminor = Integer.parseInt(project.property("aws-advanced-jdbc-wrapper.version.subminor").toString()) + if (project.property("snapshot") == "true") 1 else 0
val buildVersion = "$versionMajor.$versionMinor.$versionSubminor" + if (project.property("snapshot") == "true") "-SNAPSHOT" else ""

allprojects {
    group = "software.amazon.jdbc"
    version = buildVersion

    repositories {
        mavenCentral()
    }

    apply(plugin = "java")
    apply(plugin = "signing")
    apply(plugin = "maven-publish")

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

    java {
        withJavadocJar()
        withSourcesJar()
        sourceCompatibility = JavaVersion.VERSION_1_8
        targetCompatibility = JavaVersion.VERSION_1_8
    }

    publishing {
        publications {
            create<MavenPublication>(project.name) {
                groupId = "software.amazon.jdbc"
                artifactId = "aws-advanced-jdbc-wrapper"
                version = buildVersion

                from(components["java"])
                suppressAllPomMetadataWarnings()

                pom {
                    simplifyXml()
                    name.set("AWS Advanced JDBC Wrapper")
                    description.set(project.description ?: "Amazon Web Services (AWS) Advanced JDBC Wrapper")
                    url.set("https://github.com/awslabs/aws-advanced-jdbc-wrapper")
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
                        connection.set("scm:git:https://github.com/awslabs/aws-advanced-jdbc-wrapper.git")
                        developerConnection.set("scm:git@github.com:awslabs/aws-advanced-jdbc-wrapper.git")
                        url.set("https://github.com/awslabs/aws-advanced-jdbc-wrapper")
                    }
                    issueManagement {
                        system.set("GitHub issues")
                        url.set("https://github.com/awslabs/aws-advanced-jdbc-wrapper/issues")
                    }
                }
            }
        }
        repositories {
            maven {
                name = "OSSRH"
                url = uri("https://aws.oss.sonatype.org/service/local/staging/deploy/maven2/")
                credentials {
                    username = System.getenv("MAVEN_USERNAME")
                    password = System.getenv("MAVEN_PASSWORD")
                }
            }

            mavenLocal()
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
            sign(publishing.publications["maven"])
        }
    }
}
