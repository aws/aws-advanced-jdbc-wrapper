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

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import org.hibernate.Hibernate;
import software.amazon.entity.Address;
import software.amazon.entity.User;

import java.sql.SQLException;

public class JpaExample {
    private static EntityManagerFactory entityManagerFactory = null;

    public static void main(String[] args) {
        try {
            Address address = new Address();
            address.setTown("Orangeville");
            address.setStreet("Faulkner");
            address.setCountry("Canada");
            address.setPostal("L1L2M2");
            insertAddress(address);
            User user = new User();
            user.setFirst("Dave");
            user.setLast("Cramer");
            user.setAddress(address);
            insertUser(user);

            while (true) {
                User user1 = getUser(user.getId());
            }
        } catch (Exception ex ){
            // exception should be a TransactionStateUnknownException on failover
            ex.printStackTrace();
        }
    }
    private static void insertUser(User user) throws Exception {
        EntityManager entityManager = openEntityManager();
        entityManager.getTransaction().begin();
        entityManager.persist( user );
        entityManager.getTransaction().commit();
    }
    private static void insertAddress(Address address) throws Exception {
        EntityManager entityManager = openEntityManager();
        entityManager.getTransaction().begin();
        entityManager.persist( address );
        entityManager.getTransaction().commit();
    }
    private static User getUser(int id) throws SQLException {
        EntityManager entityManager = openEntityManager();

        User user = entityManager.find( User.class, id );
        Hibernate.initialize(user);
        entityManager.close();
        return user;
    }

    private static EntityManager openEntityManager() {
        if (entityManagerFactory == null) {
            entityManagerFactory = Persistence.createEntityManagerFactory( "Atlas" );
        }
        return entityManagerFactory.createEntityManager();
    }
}
