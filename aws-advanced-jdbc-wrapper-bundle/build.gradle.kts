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

plugins {
    id("biz.aQute.bnd.builder")
    id("com.github.johnrengelman.shadow") version "7.0.0"
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("software.amazon.awssdk:rds:2.25.12")
    implementation("software.amazon.awssdk:sts:2.25.17")
    implementation(project(":aws-advanced-jdbc-wrapper"))
}

tasks.shadowJar {

    archiveBaseName.set("aws-advanced-jdbc-wrapper")
    archiveClassifier.set("bundle-federated-auth")
    destinationDirectory.set(project(":aws-advanced-jdbc-wrapper").buildDir.resolve("libs"))

    mergeServiceFiles("META-INF")

    relocate("au", "shaded.au")
    relocate("com", "shaded.com")
    relocate("io", "shaded.io")
    relocate("org", "shaded.org")

    relocate("software", "shaded.software") {
        exclude("software.amazon.jdbc.**")
    }
}

// Copies the contents from the :aws-advanced-jdbc-wrapper jar task to ensure this jar task does not create and publish an empty jar
tasks.jar {
    dependsOn(project(":aws-advanced-jdbc-wrapper").tasks["jar"])
    bundle {
        bnd(
            """
            -exportcontents: software.*
            -removeheaders: Created-By
            Bundle-Description: Amazon Web Services (AWS) Advanced JDBC Wrapper Driver
            Bundle-DocURL: https://github.com/awslabs/aws-advanced-jdbc-wrapper
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
    from(zipTree(project(":aws-advanced-jdbc-wrapper").tasks["jar"].outputs.files.asPath))
}
