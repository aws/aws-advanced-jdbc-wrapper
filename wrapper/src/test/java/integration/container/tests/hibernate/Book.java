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

import java.util.ArrayList;
import java.util.List;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToMany;
import org.hibernate.Hibernate;
import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;

@Entity
public class Book {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private int id;

  private String title;

  private int year;

  @ManyToMany
  @Fetch(FetchMode.SUBSELECT)
  private List<Author> authors = new ArrayList<>();

  public Book() {
  }

  public Book(String title, int year) {
    this.title = title;
    this.year = year;
  }

  public int getId() {
    return this.id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getTitle() {
    return this.title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public int getYear() {
    return this.year;
  }

  public void setYear(int year) {
    this.year = year;
  }

  public List<Author> getAuthors() {
    return this.authors;
  }

  public void setAuthors(List<Author> authors) {
    this.authors = authors;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("\nTitle: ").append(this.title);
    sb.append("\nYear: ").append(this.year);
    if (Hibernate.isInitialized(this.authors)) {
      for (Author author : this.authors) {
        sb.append("\nAuthor: ").append(author.getName()).append(" (").append(author.getCountry()).append(")");
      }
    }
    return sb.toString();
  }
}
