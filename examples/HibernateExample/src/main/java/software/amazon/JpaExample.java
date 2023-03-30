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

package software.amazon;

import java.time.Duration;
import java.time.Instant;
import org.hibernate.Hibernate;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import software.amazon.entity.Address;
import software.amazon.entity.User;

public class JpaExample {

  private static final EntityManagerFactory entityManagerFactory = Persistence.createEntityManagerFactory("Atlas");

  public static void main(String[] args) {
    final Instant start = Instant.now();
    final Duration threshold = Duration.ofMinutes(5);

    int i = 0;
    try {
      // Keep inserting data for 5 minutes.
      while (Duration.between(start, Instant.now()).toMinutes() > threshold.toMinutes()) {
        Address address = new Address();
        address.setTown("Orangeville");
        address.setStreet("Faulkner");
        address.setCountry("Canada");
        address.setPostal("L1L2M2");

        insertAddress(address);

        User user = new User();
        user.setFirst("First" + i);
        user.setLast("Last" + i);
        user.setAddress(address);
        insertUser(user);

        i++;
      }
    } catch (Exception ex) {
      // Exception should be a FailoverSuccessSQLException if failover occurred.
      // For more information regarding FailoverSuccessSQLException please visit the driver's documentation.
      ex.printStackTrace();
    }
  }

  private static void insertUser(User user) {
    try (EntityManager entityManager = openEntityManager()) {
      entityManager.getTransaction().begin();
      entityManager.persist(user);
      entityManager.getTransaction().commit();
    }
  }

  private static void insertAddress(Address address) {
    try (EntityManager entityManager = openEntityManager()) {
      entityManager.getTransaction().begin();
      entityManager.persist(address);
      entityManager.getTransaction().commit();
    }
  }

  private static User getUser(int id) {
    try (EntityManager entityManager = openEntityManager()) {

      User user = entityManager.find(User.class, id);
      Hibernate.initialize(user);

      return user;
    }
  }

  private static EntityManager openEntityManager() {
    return entityManagerFactory.createEntityManager();
  }
}
