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

package example.mvp;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

@Entity
public class MVP {

  @Id
  @GeneratedValue
  private int mvpId;

  private int status;

  public MVP() {
    super();
  }

  public MVP(int id, int status) {
    super();
    this.mvpId = id;
    this.status = status;
  }

  public MVP(int status) {
    super();
    this.status = status;
  }

  public int getMvpId() {
    return mvpId;
  }

  public void setMvpId(int id) {
    this.mvpId = id;
  }

  public int getStatus() {
    return status;
  }

  public void setStatus(int name) {
    this.status = name;
  }

  @Override
  public String toString() {
    return String.format("Mvp [id=%s, status=%s]", mvpId, status);
  }
}
