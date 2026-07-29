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

package software.amazon.jdbc.authentication;

import java.lang.reflect.Method;
import java.util.Optional;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.profiles.Profile;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.regions.Region;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.StringUtils;

/**
 * Builds an {@link AwsCredentialsProvider} for AWS profiles that authenticate via a
 * {@code login_session} entry (the AWS Sign-In / browser-login credential flow).
 *
 * <p>The AWS SDK resolves {@code login_session} credentials through a {@code LoginCredentialsProvider}
 * whose region is carried by an internally constructed {@code SigninClient}. When that provider is
 * built through the profile-credentials factory (i.e. through {@code DefaultCredentialsProvider}),
 * the {@code SigninClient} region is resolved via the SDK's {@code DefaultAwsRegionProviderChain},
 * which ignores the configured AWS profile and falls back to the {@code default} profile. This
 * factory instead constructs the provider with a {@code SigninClient} scoped to the region that the
 * driver already resolved for the connection, so the configured profile is honored.
 *
 * <p>The {@code software.amazon.awssdk:signin} module is optional and is therefore accessed purely
 * via reflection. This class only reaches the reflective code path when the connected profile
 * actually declares a {@code login_session} entry.
 */
public class LoginCredentialsProviderFactory {

  static final String LOGIN_SESSION_PROFILE_PROPERTY = "login_session";

  private static final String DEFAULT_PROFILE_NAME = "default";
  private static final String SIGNIN_CLIENT_CLASS =
      "software.amazon.awssdk.services.signin.SigninClient";
  private static final String SIGNIN_CLIENT_BUILDER_CLASS =
      "software.amazon.awssdk.services.signin.SigninClientBuilder";
  private static final String LOGIN_CREDENTIALS_PROVIDER_CLASS =
      "software.amazon.awssdk.services.signin.auth.LoginCredentialsProvider";
  private static final String LOGIN_CREDENTIALS_PROVIDER_BUILDER_CLASS =
      "software.amazon.awssdk.services.signin.auth.LoginCredentialsProvider$Builder";

  /**
   * Attempts to build a login-session credentials provider for the given profile.
   *
   * @param awsProfileName the configured AWS profile name, or null/empty to use the default profile.
   * @param region         the region resolved for the connection, used to scope the signin exchange.
   * @return a login-session credentials provider, or null if the profile does not use a
   *     {@code login_session} entry (in which case the caller should fall back to the standard
   *     credentials provider).
   * @throws RuntimeException if the profile uses a {@code login_session} entry but the login flow
   *     cannot be set up (signin module missing, region unknown, or reflective construction failed).
   */
  public static @Nullable AwsCredentialsProvider create(
      final @Nullable String awsProfileName, final @Nullable Region region) {

    final String loginSession = getLoginSession(awsProfileName);
    if (StringUtils.isNullOrEmpty(loginSession)) {
      // Not a login_session profile; caller should use the standard credentials provider.
      return null;
    }

    if (!isSigninModuleAvailable()) {
      throw new RuntimeException(
          Messages.get("AwsCredentialsManager.signinModuleNotInClasspath"));
    }

    if (region == null) {
      throw new RuntimeException(
          Messages.get("AwsCredentialsManager.loginSessionRegionRequired"));
    }

    return buildLoginCredentialsProvider(loginSession, region);
  }

  /**
   * Reads the {@code login_session} property of the given profile from the default profile files.
   *
   * @param awsProfileName the profile name, or null/empty to use the default profile.
   * @return the {@code login_session} value, or null if the profile is absent or has no such entry.
   */
  static @Nullable String getLoginSession(final @Nullable String awsProfileName) {
    final String profileName =
        StringUtils.isNullOrEmpty(awsProfileName) ? DEFAULT_PROFILE_NAME : awsProfileName;
    try {
      final ProfileFile profileFile = ProfileFile.defaultProfileFile();
      final Optional<Profile> profile = profileFile.profile(profileName);
      if (!profile.isPresent()) {
        return null;
      }
      return profile.get().properties().get(LOGIN_SESSION_PROFILE_PROPERTY);
    } catch (final RuntimeException e) {
      // A malformed or unreadable profile file must not be interpreted as a login_session profile;
      // let the caller fall back to the standard credentials provider.
      return null;
    }
  }

  static boolean isSigninModuleAvailable() {
    try {
      Class.forName(LOGIN_CREDENTIALS_PROVIDER_CLASS);
      Class.forName(SIGNIN_CLIENT_CLASS);
      return true;
    } catch (final ClassNotFoundException e) {
      return false;
    }
  }

  // Method.invoke types its receiver (arg0) and its varargs elements (arg1...) as non-null, but a
  // static factory method is invoked with a null receiver and the reflective results used as
  // receivers/arguments here are typed as possibly-null by the JDK annotations. The builder methods
  // never return null, and a null from any of them would surface as the same
  // ReflectiveOperationException/NullPointerException handling as before, so the arguments are
  // passed through unchanged.
  @SuppressWarnings("argument")
  private static AwsCredentialsProvider buildLoginCredentialsProvider(
      final String loginSession, final Region region) {
    try {
      // SigninClient signinClient = SigninClient.builder().region(region).build();
      final Class<?> signinClientClass = Class.forName(SIGNIN_CLIENT_CLASS);
      final Class<?> signinClientBuilderClass = Class.forName(SIGNIN_CLIENT_BUILDER_CLASS);
      final @Nullable Object signinClientBuilder = signinClientClass.getMethod("builder").invoke(null);
      signinClientBuilderClass.getMethod("region", Region.class)
          .invoke(signinClientBuilder, region);
      final @Nullable Object signinClient =
          signinClientBuilderClass.getMethod("build").invoke(signinClientBuilder);

      // LoginCredentialsProvider.builder()
      //     .signinClient(signinClient)
      //     .loginSession(loginSession)
      //     .build();
      final Class<?> loginProviderClass = Class.forName(LOGIN_CREDENTIALS_PROVIDER_CLASS);
      final Class<?> loginProviderBuilderClass =
          Class.forName(LOGIN_CREDENTIALS_PROVIDER_BUILDER_CLASS);
      final @Nullable Object loginProviderBuilder = loginProviderClass.getMethod("builder").invoke(null);
      loginProviderBuilderClass.getMethod("signinClient", signinClientClass)
          .invoke(loginProviderBuilder, signinClient);
      loginProviderBuilderClass.getMethod("loginSession", String.class)
          .invoke(loginProviderBuilder, loginSession);
      final Method buildMethod = loginProviderBuilderClass.getMethod("build");
      final @Nullable Object provider = buildMethod.invoke(loginProviderBuilder);

      if (provider == null) {
        // build() never returns null; a null here would otherwise be reported to the caller as
        // "no login_session profile", silently falling back to the standard credentials provider.
        throw new RuntimeException(
            Messages.get("AwsCredentialsManager.loginSessionProviderNull"));
      }

      return (AwsCredentialsProvider) provider;
    } catch (final ReflectiveOperationException e) {
      throw new RuntimeException(
          Messages.get("AwsCredentialsManager.loginSessionProviderFailed", new Object[] {e}), e);
    }
  }
}
