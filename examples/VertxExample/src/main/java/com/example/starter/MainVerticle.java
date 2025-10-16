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

package com.example.starter;

import com.example.starter.model.Instance;
import com.example.starter.model.Example;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;

import java.util.ArrayList;
import java.util.List;

public class MainVerticle extends AbstractVerticle {
  public static final String UPDATE = "UPDATE example SET status = ? WHERE id = ?";
  public static final String INSERT = "INSERT INTO example (status, id) VALUES (?, ?)";

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

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    Router router = Router.router(vertx);
    router.get("/id").handler(this::getCurrentInstance);
    router.get("/topology").handler(this::getTopology);
    router.get("/fetch/:rows").handler(this::fetchAll);
    router.get("/insert/:rows").handler(this::insert);
    router.get("/update/:rows").handler(this::update);

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

  private void getCurrentInstance(RoutingContext routingContext) {
    write.query("select pg_catalog.aurora_db_instance_identifier() as id, case when pg_catalog.pg_is_in_recovery() then 'reader' else 'writer' end as role").execute().onSuccess(
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

  private void getTopology(RoutingContext routingContext) {
    write.query("select server_id from pg_catalog.aurora_replica_status()").execute().onSuccess(
      rows -> {
        ArrayList<String> arr = new ArrayList<>();
        for (Row row : rows) {
          arr.add(row.getString("server_id"));
        }

        routingContext.response()
            .putHeader("content-type", "application/json")
            .setStatusCode(200)
            .end(Json.encodePrettily(arr));
      })
      .onFailure(e -> {
        System.out.println("failure: " + e);
        // handle the failure
      });
  }

  private void insert(RoutingContext routingContext) {
    int rowSize = Integer.parseInt(routingContext.request().getParam("rows"));

    final List<Tuple> inputs = new ArrayList<>();
    for (int i = 0; i < rowSize; i++) {
      inputs.add(Tuple.of(getRandomNumber(1, 800), getRandomNumber(0, 4)));
    }

    write
      .preparedQuery(INSERT)
      .executeBatch(inputs)
      .onSuccess(rows -> {
        for (Row row : rows) {
          System.out.println("row: " + row);
        }

        routingContext.response()
          .putHeader("content-type", "plain-text")
          .setStatusCode(200)
          .end("inserted " + rowSize);
      })
      .onFailure(e -> {
        System.out.println("failure: " + e);
        // handle the failure
      });
  }

  private void fetchAll(RoutingContext routingContext) {
    String rowSize = routingContext.request().getParam("rows");
    if (rowSize == null) {
      rowSize = "10";
    }
    JsonArray arr = new JsonArray();

    read
      .query("SELECT id, status FROM example LIMIT " + rowSize)
      .execute()
      .onFailure(e -> {
        System.out.println("failure: " + e);
        // handle the failure
      })
      .onSuccess(rows -> {
        for (Row row : rows) {
          Example example = new Example(row.getInteger("id"), row.getInteger("status"));
          arr.add(example);
        }

        routingContext.response()
          .putHeader("content-type", "application/json")
          .setStatusCode(200)
          .end(arr.encodePrettily());
      });
  }

  private void update(RoutingContext routingContext) {
    int rowSize = Integer.parseInt(routingContext.request().getParam("rows"));

    final List<Tuple> inputs = new ArrayList<>();
    for (int i = 0; i < rowSize; i++) {
      inputs.add(Tuple.of(getRandomNumber(1, 800), getRandomNumber(0, 4)));
    }

    write
      .preparedQuery(UPDATE)
      .executeBatch(inputs)
      .onSuccess(rows -> {
        for (Row row : rows) {
          System.out.println("row: " + row);
        }
        routingContext.response()
          .putHeader("content-type", "application/json")
          .setStatusCode(200)
          .end();
      })
      .onFailure(e -> {
        System.out.println("failure: " + e);
        // handle the failure
      });
  }

  int getRandomNumber(int min, int max) {
    return (int) ((Math.random() * (max - min)) + min);
  }
}
