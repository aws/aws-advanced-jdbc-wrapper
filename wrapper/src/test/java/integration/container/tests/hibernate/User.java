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
import javax.persistence.Table;
import org.hibernate.Hibernate;
import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;
import org.hibernate.annotations.NaturalId;

@Entity
@Table(name = "Users") // "user" is a reserved keyword in Postgresql so we replace it with a less conflicting "users".
public class User {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private int id;

  @NaturalId
  private String email;

  private String name;

  private String phone;

  @ManyToMany
  @Fetch(FetchMode.SUBSELECT)
  private List<Skill> skills = new ArrayList<>();

  @ManyToMany
  private List<Tool> tools = new ArrayList<>();

  public int getId() {
    return this.id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getEmail() {
    return this.email;
  }

  public void setEmail(String email) {
    this.email = email;
  }

  public String getName() {
    return this.name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getPhone() {
    return this.phone;
  }

  public void setPhone(String phone) {
    this.phone = phone;
  }

  public List<Skill> getSkills() {
    return this.skills;
  }

  public void setSkills(List<Skill> skills) {
    this.skills = skills;
  }

  public List<Tool> getTools() {
    return this.tools;
  }

  public void setTools(List<Tool> tools) {
    this.tools = tools;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("\nName: ").append(this.name);
    sb.append("\nEmail: ").append(this.email);
    sb.append("\nPhone: ").append(this.phone);
    if (Hibernate.isInitialized(this.tools)) {
      for (Tool tool : this.tools) {
        sb.append("\nTool: ").append(tool.getName());
      }
    }
    if (Hibernate.isInitialized(this.skills)) {
      for (Skill skill : this.skills) {
        sb.append("\nSkill: ").append(skill.getName());
      }
    }
    return sb.toString();
  }
}
