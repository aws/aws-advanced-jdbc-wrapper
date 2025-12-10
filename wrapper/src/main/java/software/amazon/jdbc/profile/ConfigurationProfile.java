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

package software.amazon.jdbc.profile;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.ConnectionProvider;
import software.amazon.jdbc.ConnectionPluginFactory;
import software.amazon.jdbc.authentication.AwsCredentialsProviderHandler;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.exceptions.ExceptionHandler;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;

public class ConfigurationProfile {

  private final @NonNull String name;
  private final @Nullable List<Class<? extends ConnectionPluginFactory>> pluginFactories;
  private final @Nullable Properties properties;
  private @Nullable Supplier<Dialect> dialectSupplier;
  private @Nullable Supplier<TargetDriverDialect> targetDriverDialectSupplier;
  private @Nullable Supplier<ExceptionHandler> exceptionHandlerSupplier;
  private @Nullable Supplier<AwsCredentialsProviderHandler> awsCredentialsProviderHandlerSupplier;
  private @Nullable Supplier<ConnectionProvider> connectionProviderSupplier;

  private @Nullable Dialect dialect;
  private @Nullable TargetDriverDialect targetDriverDialect;
  private @Nullable ExceptionHandler exceptionHandler;
  private @Nullable AwsCredentialsProviderHandler awsCredentialsProviderHandler;
  private @Nullable ConnectionProvider connectionProvider;

  private final ReentrantLock lock = new ReentrantLock();

  ConfigurationProfile(final @NonNull String name,
      @Nullable List<Class<? extends ConnectionPluginFactory>> pluginFactories,
      @Nullable Properties properties,
      @Nullable Supplier<Dialect> dialectSupplier,
      @Nullable Supplier<TargetDriverDialect> targetDriverDialectSupplier,
      @Nullable Supplier<ExceptionHandler> exceptionHandlerSupplier,
      @Nullable Supplier<ConnectionProvider> connectionProviderSupplier,
      @Nullable Supplier<AwsCredentialsProviderHandler> credentialsProviderHandlerSupplier) {

    this.name = name;
    this.pluginFactories = pluginFactories;
    this.properties = properties;
    this.dialectSupplier = dialectSupplier;
    this.targetDriverDialectSupplier = targetDriverDialectSupplier;
    this.exceptionHandlerSupplier = exceptionHandlerSupplier;
    this.connectionProviderSupplier = connectionProviderSupplier;
    this.awsCredentialsProviderHandlerSupplier = credentialsProviderHandlerSupplier;
  }

  ConfigurationProfile(final @NonNull String name,
      @Nullable List<Class<? extends ConnectionPluginFactory>> pluginFactories,
      @Nullable Properties properties,
      @Nullable Dialect dialect,
      @Nullable TargetDriverDialect targetDriverDialect,
      @Nullable ExceptionHandler exceptionHandler,
      @Nullable ConnectionProvider connectionProvider,
      @Nullable AwsCredentialsProviderHandler credentialsProviderHandler) {

    this.name = name;
    this.pluginFactories = pluginFactories;
    this.properties = properties;
    this.dialect = dialect;
    this.targetDriverDialect = targetDriverDialect;
    this.exceptionHandler = exceptionHandler;
    this.connectionProvider = connectionProvider;
    this.awsCredentialsProviderHandler = credentialsProviderHandler;
  }

  public @NonNull String getName() {
    return this.name;
  }

  public @Nullable Properties getProperties() {
    return this.properties;
  }

  public @Nullable List<Class<? extends ConnectionPluginFactory>> getPluginFactories() {
    return this.pluginFactories;
  }

  public @Nullable Dialect getDialect() {
    if (this.dialect != null) {
      return this.dialect;
    }
    if (this.dialectSupplier == null) {
      return null;
    }

    this.lock.lock();
    try {
      this.dialect = this.dialectSupplier.get();
      return this.dialect;
    } finally {
      this.lock.unlock();
    }
  }

  public @Nullable TargetDriverDialect getTargetDriverDialect() {
    if (this.targetDriverDialect != null) {
      return this.targetDriverDialect;
    }
    if (this.targetDriverDialectSupplier == null) {
      return null;
    }
    try {
      this.lock.lock();
      if (this.targetDriverDialect != null) {
        return this.targetDriverDialect;
      }
      this.targetDriverDialect = this.targetDriverDialectSupplier.get();
      return this.targetDriverDialect;
    } finally {
      this.lock.unlock();
    }
  }

  public @Nullable ExceptionHandler getExceptionHandler() {
    if (this.exceptionHandler != null) {
      return this.exceptionHandler;
    }
    if (this.exceptionHandlerSupplier == null) {
      return null;
    }
    try {
      this.lock.lock();
      if (this.exceptionHandler != null) {
        return this.exceptionHandler;
      }
      this.exceptionHandler = this.exceptionHandlerSupplier.get();
      return this.exceptionHandler;
    } finally {
      this.lock.unlock();
    }
  }

  public @Nullable ConnectionProvider getConnectionProvider() {
    if (this.connectionProvider != null) {
      return this.connectionProvider;
    }
    if (this.connectionProviderSupplier == null) {
      return null;
    }
    try {
      this.lock.lock();
      if (this.connectionProvider != null) {
        return this.connectionProvider;
      }
      this.connectionProvider = this.connectionProviderSupplier.get();
      return this.connectionProvider;
    } finally {
      this.lock.unlock();
    }
  }

  public @Nullable AwsCredentialsProviderHandler getAwsCredentialsProviderHandler() {
    if (this.awsCredentialsProviderHandler != null) {
      return this.awsCredentialsProviderHandler;
    }
    if (this.awsCredentialsProviderHandlerSupplier == null) {
      return null;
    }
    try {
      this.lock.lock();
      if (this.awsCredentialsProviderHandler != null) {
        return this.awsCredentialsProviderHandler;
      }
      this.awsCredentialsProviderHandler = this.awsCredentialsProviderHandlerSupplier.get();
      return this.awsCredentialsProviderHandler;
    } finally {
      this.lock.unlock();
    }
  }
}
