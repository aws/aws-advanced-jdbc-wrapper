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

package integration.container.tests.hibernate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import integration.TestEnvironmentFeatures;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriver;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestFeature;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import org.hibernate.Hibernate;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.query.Query;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@TestMethodOrder(MethodOrderer.MethodName.class)
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY})
@Order(19)
public class HibernateTests {
  private static final Logger LOGGER = Logger.getLogger(HibernateTests.class.getName());

  @TestTemplate
  @ExtendWith(TestDriverProvider.class)
  public void testSimpleTest() {

    final Configuration configuration = this.getConfiguration();
    try (SessionFactory sessionFactory = configuration.buildSessionFactory(
            new StandardServiceRegistryBuilder()
                .applySettings(configuration.getProperties())
                .build());
        Session session = sessionFactory.openSession()) {

      Tool tool = new Tool();
      tool.setName("Hammer");
      this.insertObject(session, tool);
      List<Tool> tools = new ArrayList<>();
      tools.add(tool);

      Skill skill = new Skill();
      skill.setName("Hammering Things");
      this.insertObject(session, skill);
      List<Skill> skills = new ArrayList<>();
      skills.add(skill);

      User user = new User();
      user.setName("Brett Meyer");
      user.setEmail("foo@foo.com");
      user.setPhone("123-456-7890");
      user.setTools(tools);
      user.setSkills(skills);

      this.insertObject(session, user);

      User savedUser = getUser(session, user.getId());

      assertNotNull(savedUser);
      assertEquals("Brett Meyer", savedUser.getName());
      assertEquals("foo@foo.com", savedUser.getEmail());
      assertEquals("123-456-7890", savedUser.getPhone());
      assertEquals(1, savedUser.getSkills().size());
      assertEquals(1, savedUser.getTools().size());

      User savedQueryUser = getQueryUser(session, user.getId());

      assertNotNull(savedQueryUser);
      assertEquals("Brett Meyer", savedQueryUser.getName());
      assertEquals("foo@foo.com", savedQueryUser.getEmail());
      assertEquals("123-456-7890", savedQueryUser.getPhone());
      assertEquals(1, savedQueryUser.getSkills().size());
      assertEquals(1, savedQueryUser.getTools().size());
    }
  }

  private Configuration getConfiguration() {

    String url = ConnectionStringHelper.getWrapperUrl();
    LOGGER.finest("Connecting to " + url);

    final Configuration configuration = new Configuration();
    configuration.addAnnotatedClass(User.class);
    configuration.addAnnotatedClass(Tool.class);
    configuration.addAnnotatedClass(Skill.class);

    switch (TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine()) {
      case PG:
        configuration.setProperty("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
        break;
      case MYSQL:
        //configuration.setProperty("hibernate.dialect", "org.hibernate.dialect.MySQL8Dialect");
        configuration.setProperty("hibernate.dialect", "org.hibernate.dialect.MySQLDialect");
        break;
      case MARIADB:
        configuration.setProperty("hibernate.dialect", "org.hibernate.dialect.MariaDBDialect");
        break;
      default:
        throw new UnsupportedOperationException(
            TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine().toString());
    }

    configuration.setProperty("hibernate.connection.driver_class", "software.amazon.jdbc.Driver");

    configuration.setProperty("hibernate.show_sql", "true");
    configuration.setProperty("hibernate.format_sql", "true");
    configuration.setProperty("hibernate.hbm2ddl.auto", "create");
    configuration.setProperty("hibernate.connection.url", url);
    configuration.setProperty("hibernate.connection.username",
        TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getUsername());
    configuration.setProperty("hibernate.connection.password",
        TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getPassword());

    // Additional Wrapper driver properties
    configuration.setProperty("hibernate.connection.wrapperPlugins", "");
    configuration.setProperty("hibernate.connection.connectTimeout", "10000");
    configuration.setProperty("hibernate.connection.socketTimeout", "10000");
    configuration.setProperty("hibernate.connection.enableTelemetry", "true");
    configuration.setProperty("hibernate.connection.telemetrySubmitToplevel", "true");
    configuration.setProperty("hibernate.connection.telemetryTracesBackend",
        TestEnvironment.getCurrent().getInfo().getRequest().getFeatures().contains(
            TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED) ? "xray" : "none");
    configuration.setProperty("hibernate.connection.telemetryMetricsBackend",
        TestEnvironment.getCurrent().getInfo().getRequest().getFeatures().contains(
            TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED) ? "otlp" : "none");
    configuration.setProperty("hibernate.connection.tcpKeepAlive", "false");

    if (TestEnvironment.getCurrent().getCurrentDriver() == TestDriver.MARIADB) {
      // This property is sometimes required when using the mariadb driver against multi-az mysql version 8.4, or you
      // will get the error "RSA public key is not available client side" when connecting. The mariadb driver may not
      // fully support mysql 8.4's SSL mechanisms, which is why this property is only required for newer mysql versions.
      configuration.setProperty("hibernate.connection.allowPublicKeyRetrieval", "true");
    }

    return configuration;
  }

  private void insertObject(Session session, Object object) {
    session.getTransaction().begin();
    session.persist(object);
    session.getTransaction().commit();
  }

  private User getUser(Session session, int id) {
    User user = session.find(User.class, id);

    Hibernate.initialize(user.getTools());
    Hibernate.initialize(user.getSkills());

    return user;
  }

  private User getQueryUser(Session session, int id) {

    CriteriaBuilder criteriaBuilder = session.getCriteriaBuilder();
    CriteriaQuery<User> criteriaQuery = criteriaBuilder.createQuery(User.class);
    Root<User> root = criteriaQuery.from(User.class);
    criteriaQuery.select(root).where(criteriaBuilder.equal(root.get("id"), id));

    Query<User> query = session.createQuery(criteriaQuery);
    User user = query.uniqueResult();

    Hibernate.initialize(user.getTools());
    Hibernate.initialize(user.getSkills());

    return user;
  }
}
