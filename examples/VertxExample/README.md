# Tutorial: Getting started with the AWS Advanced Jdbc Driver and Vert.x

In this tutorial, you will set up a Vert.x application with the AWS JDBC Driver, and use the driver to execute some simple database operations on an Aurora PostgreSQL database.

> Note: this tutorial was written using the following technologies:
>    - AWS JDBC Driver 2.3.5
>    - PostgreSQL 42.5.4
>    - Java 8
>    - Vert.x 4.4.2

You will progress through the following sections:
1. Create a Gradle project
2. Add the required Gradle dependencies
3. Set up connection pools
4. Set up routing
5. Set up the start method
6. Run the application

## Step 1: Create a Gradle Project
Create a Gradle Project with the following project hierarchy:
```
├───build.gradle
└───src
    └───main
        └───java
            └───com
                └───example
                    └───starter
                        ├───model
                        │   └───Example.java
                        │   └───Instance.java
                        └───VertxExampleApplication.java
```

You can also use the Vert.x initializer to generate a starter project:
1. Go to https://start.vertx.io/.
2. Select the Vert.x version, Java, and Gradle.
3. Add the following dependencies: 
   - JDBC Client
   - Vert.x Config
   - Vert.x Web
4. Generate the project.

## Step 2: Add the required Gradle dependencies
Your `build.gradle.kts` file should include the following dependencies:

```gradle
dependencies {
    implementation(platform("io.vertx:vertx-stack-depchain:4.4.2"))
    implementation("io.vertx:vertx-core")
    implementation("io.vertx:vertx-config")
    implementation("io.vertx:vertx-jdbc-client")
    implementation("io.vertx:vertx-web")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.12.7.1")
    implementation("org.postgresql:postgresql:42.5.4")
    implementation("software.amazon.jdbc:aws-advanced-jdbc-wrapper:latest")
}
```

Please note that the sample code inside the AWS JDBC Driver project will use the dependency `implementation(project(":aws-advanced-jdbc-wrapper"))` instead of `implementation("software.amazon.jdbc:aws-advanced-jdbc-wrapper:latest")` as seen above.

## Step 3: Set up connection pools
Vert.x applications can use connection pools to handle requests. In this example, we use two connection pools, one to handle write requests and another to handle read requests. Vert.x uses C3P0 by default to manage database connections.

```java
final JsonObject writeConfig = new JsonObject()
    .put("url", "jdbc:aws-wrapper:postgresql://db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com:5432/postgres")
    .put("driverClass", "software.amazon.jdbc.Driver")
    .put("user", "username")
    .put("password", "password")
    .put("testConnectionOnCheckout", "false")
    .put("testConnectionOnCheckin", "false")
    .put("idleConnectionTestPeriod", "30")
    .put("numHelperThreads", "10")
    .put("max_pool_size", 20);

final JsonObject readConfig = new JsonObject()
    .put("url", "jdbc:aws-wrapper:postgresql://db-identifier.cluster-ro-XYZ.us-east-2.rds.amazonaws.com:5432/postgres")
    .put("driverClass", "software.amazon.jdbc.Driver")
    .put("user", "username")
    .put("password", "password")
    .put("testConnectionOnCheckout", "false")
    .put("testConnectionOnCheckin", "false")
    .put("idleConnectionTestPeriod", "30")
    .put("numHelperThreads", "10")
    .put("max_pool_size", 20);

JDBCPool write;
JDBCPool read;
```

## Step 4: Set up Routing
To set up the HTTP server, you can first create a router. This tutorial will only walk through creating one route, which will obtain the instance ID of the currently connected database node. However, the sample code will contain examples of different routes.

```java
Router router = Router.router(vertx);
router.get("/id").handler(this::getCurrentInstance);
```

This will map requests to certain paths to methods you will define. This particular requests maps to the method `getCurrentInstance`, which should look like this:

```java
private void getCurrentInstance(RoutingContext routingContext) {
    write.query("select aurora_db_instance_identifier() as id, case when pg_is_in_recovery() then 'reader' else 'writer' end as role")
    .execute()
    .onSuccess(
        rows -> {
            for (Row row : rows) {
                Instance instance = new Instance(
                row.getString("id"), row.getString("role"));
            
                routingContext.response()
                .putHeader("content-type", "application/json")
                .setStatusCode(200)
                .end(Json.encodePrettily(instance));
            }
        })
    .onFailure(e -> {
    System.out.println("failure: " + e);
      // handle the failure
    });
}
```

## Step 5: Set up start method
The `start` method should be set up to create the Vert.x server and the connection pools. At this point, it should look like this:

```java
@Override
public void start(Promise<Void> startPromise) throws Exception {
    Router router = Router.router(vertx);
    router.get("/id").handler(this::getCurrentInstance);
    
    write = JDBCPool.pool(vertx, writeConfig);
    read = JDBCPool.pool(vertx, readConfig);
    
    vertx.createHttpServer()
      .requestHandler(router)
      .listen(8888, http -> {
        if (http.succeeded()) {
          try {
            startPromise.complete();
            System.out.println("HTTP server started on port 8888");
          } catch (IllegalStateException e) {
            startPromise.fail(http.cause());
          }
        }
    });
}
```

## Step 4: Run the Application
Start the application by running the command `./gradlew :vertxexample:run`. Once the application has started, requests can be made using the previously defined routes. For example, to obtain the current instance ID, the command `curl localhost:8888/id` can be run.

# Summary
This tutorial walks through the steps required to add and configure the AWS JDBC Driver to a simple Vert.x application.
