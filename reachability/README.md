### Instructions

1. Download GraalVM 21 and set your environment variables to use it as your java version. Note that there is [an open bug](https://github.com/oracle/graal/issues/9929#issuecomment-2483039935) with GraalVM 23 that will cause errors when generating the reachability metadata
2. Navigate into this directory (eg `path/to/project/aws-advanced-jdbc-wrapper/reachability`)
3. Execute `../gradlew -Pagent test` to run the tests with the GraalVM native image agent
4. Execute `../gradlew metadataCopy`
5. The metadata json files will be in the `metadata-output` directory
