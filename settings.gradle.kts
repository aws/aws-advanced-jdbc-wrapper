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

rootProject.name = "jdbc-proxy-driver"

include(
    "aws-jdbc-proxy-driver"
)

project(":aws-jdbc-proxy-driver").projectDir = file("driver-proxy")

pluginManagement {
    plugins {
        fun String.v() = extra["$this.version"].toString()
        fun PluginDependenciesSpec.idv(id: String, key: String = id) = id(id) version key.v()

        id("com.github.spotbugs") version "5.0.+"
        id("com.diffplug.spotless") version "6.8.+"
        id("com.github.vlsi.gradle-extensions") version "1.+"
        id("com.github.vlsi.stage-vote-release") version "1.+"
        id("com.github.vlsi.ide") version "1.+"
        id("me.champeau.gradle.jmh") version "0.+"
        id("org.checkerframework") version "0.+"
    }
}
