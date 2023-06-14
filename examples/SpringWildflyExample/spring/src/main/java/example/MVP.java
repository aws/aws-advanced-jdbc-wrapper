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

package example;

public class MVP {

  int status;
  int mvpId;

  public MVP(int status, int mvpId) {
    this.status = status;
    this.mvpId = mvpId;
  }

  public int getStatus() {
    return status;
  }

  public void setStatus(int status) {
    this.status = status;
  }

  public int getMvpId() {
    return mvpId;
  }

  public void setMvpId(int mvpId) {
    this.mvpId = mvpId;
  }

  @Override
  public String toString() {
    return "MVP{" +
        "status=" + status +
        ", mvpId='" + mvpId +
        '}';
  }
}
