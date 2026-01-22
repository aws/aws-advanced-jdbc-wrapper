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

package software.amazon.jdbc.states;

import java.util.Optional;

public class SessionStateField<T> {
  private Optional<T> value = Optional.empty();
  private Optional<T> pristineValue = Optional.empty();

  public SessionStateField<T> copy() {
    final SessionStateField<T> newField = new SessionStateField<>();
    if (this.value.isPresent()) {
      newField.setValue(this.value.get());
    }
    if (this.pristineValue.isPresent()) {
      newField.setPristineValue(this.pristineValue.get());
    }
    return newField;
  }

  public Optional<T> getValue() {
    return this.value;
  }

  public Optional<T> getPristineValue() {
    return this.pristineValue;
  }

  public void setValue(final T value) {
    this.value = Optional.ofNullable(value);
  }

  public void setPristineValue(final T value) {
    this.pristineValue = Optional.ofNullable(value);
  }

  public void resetValue() {
    this.value = Optional.empty();
  }

  public void resetPristineValue() {
    this.pristineValue = Optional.empty();
  }

  public void reset() {
    this.resetValue();
    this.resetPristineValue();
  }

  public boolean isPristine() {
    // the value has never been set up so the session state has pristine value
    if (!this.value.isPresent()) {
      return true;
    }

    // the pristine value isn't setup, so it's inconclusive.
    // take the safest path
    if (!this.pristineValue.isPresent()) {
      return false;
    }

    return this.value.get().equals(this.pristineValue.get());
  }

  public boolean canRestorePristine() {
    if (!this.pristineValue.isPresent()) {
      return false;
    }
    if (this.value.isPresent()) {
      // it's necessary to restore pristine value only if current session value is not the same as pristine value.
      return !(this.value.get().equals(this.pristineValue.get()));
    }

    // it's inconclusive if the current value is the same as pristine value, so we need to take the safest path.
    return true;
  }

  @Override
  public String toString() {
    return String.format("%s -> %s",
        pristineValue.isPresent() ? pristineValue.get() : "(blank)",
        value.isPresent() ? value.get() : "(blank)");
  }
}
