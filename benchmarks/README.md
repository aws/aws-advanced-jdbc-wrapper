# Benchmarks for AWS JDBC Proxy Driver

## Usage
1. Build the benchmarks with the following command `../gradlew jmhJar`.
   1. the JAR file will be outputted to `build/libs`
2. Run the benchmarks with the following command `java -jar build/libs/benchmarks-0.1.0-SNAPSHOT-jmh.jar`.
   1. you may have to update the command based on the exact version of the produced JAR file
