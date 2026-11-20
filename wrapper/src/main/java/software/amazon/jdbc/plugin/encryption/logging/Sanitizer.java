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

package software.amazon.jdbc.plugin.encryption.logging;

import java.util.Locale;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;

/**
 * Redaction helpers shared by the encryption plugin's logs, error context and exception messages,
 * so that a key id, an ARN or a connection URL is masked the same way wherever it appears. Every
 * method returns null for null input, so callers decide how to render a missing value. None of
 * them use backtracking regular expressions, so they run in linear time on any input.
 *
 * <p>What is masked: master key ARNs and key ids (the account, the region and the middle of the
 * key id), aliases, other ARNs, the user info and password parameters of a connection URL, and
 * the value of a password-like "name=value" pair. Control characters are replaced everywhere, so
 * a value cannot forge log lines. Nothing else is touched: table and column names, SQL, primary
 * keys and ordinary error text are logged as before.
 */
public final class Sanitizer {

  private static final String ARN_PREFIX = "arn:aws:kms:***:***:key/";
  private static final String ALIAS_ARN = "arn:aws:kms:***:***:alias/***";
  private static final String OTHER_ARN = "arn:***";
  private static final String MASK = "***";
  private static final int MAX_ERROR_MESSAGE_LENGTH = 200;
  // Names whose value is masked in a "name=value" pair, matched anywhere in the name, lower case.
  private static final String[] SECRET_NAMES = {"password", "passwd", "pwd", "secret", "token", "credential"};

  private Sanitizer() {
  }

  /**
   * Masks a master key as the plugin accepts it: a KMS key ARN, "key/&lt;id&gt;", a bare key id or
   * an alias. The account and region are dropped and the middle of the key id is masked the same
   * way {@link #keyId} does, so a key id is redacted consistently wherever it appears. An alias is
   * a name chosen by people, so none of it is kept, and an ARN that is not a KMS ARN (a role, say)
   * is replaced whole. Control characters are replaced, since the value may come from a
   * key_storage row that someone rewrote and a newline would forge log lines.
   *
   * @param value the master key ARN, alias, or key id
   * @return the masked value, or null for null
   */
  public static @Nullable String arn(final @Nullable String value) {
    if (value == null) {
      return null;
    }
    final String lower = value.trim().toLowerCase(Locale.ROOT);
    if (lower.startsWith("alias/") || lower.contains(":alias/")) {
      return ALIAS_ARN;
    }
    if (lower.startsWith("arn:") && !isKmsArn(lower)) {
      return OTHER_ARN;
    }
    // The key id is the part after the last '/', or the whole value when there is none.
    final String id = value.substring(value.lastIndexOf('/') + 1);
    return ARN_PREFIX + (id.isEmpty() ? MASK : keyId(id));
  }

  /**
   * Masks every entry of a comma-separated list of master keys with {@link #arn}.
   *
   * @param commaSeparated the list, as configured
   * @return the masked entries, comma-separated, or null for null
   */
  public static @Nullable String arnList(final @Nullable String commaSeparated) {
    if (commaSeparated == null) {
      return null;
    }
    final StringBuilder sb = new StringBuilder();
    int start = 0;
    while (start <= commaSeparated.length()) {
      int end = commaSeparated.indexOf(',', start);
      if (end < 0) {
        end = commaSeparated.length();
      }
      final String entry = commaSeparated.substring(start, end).trim();
      if (!entry.isEmpty()) {
        if (sb.length() > 0) {
          sb.append(", ");
        }
        sb.append(arn(entry));
      }
      start = end + 1;
    }
    return sb.toString();
  }

  /**
   * Keeps the first and last four characters of a key id and masks the middle. Control characters
   * are replaced.
   *
   * @param value the key id
   * @return the masked value, or null for null
   */
  public static @Nullable String keyId(final @Nullable String value) {
    if (value == null) {
      return null;
    }
    if (value.length() > 8) {
      return replaceControlCharacters(value.substring(0, 4)) + MASK
          + replaceControlCharacters(value.substring(value.length() - 4));
    }
    return MASK;
  }

  /**
   * Masks credentials embedded in a connection URL: the user info and the value of every
   * password-like parameter. The user info is everything between "://" and the last '@' before
   * the next '?', '#' or whitespace, found in a single pass, so a crafted URL cannot make this
   * quadratic.
   *
   * <p>This deliberately over-masks. RFC 3986 ends the user info at the first '/', but a password
   * with a raw '/' (the drivers expect it percent-encoded, yet people write it unencoded) then
   * looks exactly like a path: "u:pa/ss@host/db" and "host/db@x" have the same shape, so no parser
   * can tell them apart. Ending at the first '/' would log such a password in clear text, while
   * going to the last '@' only costs a hidden host when a path contains '@' (it prints
   * "***@x"). A log that hides too much is a nuisance; one that shows a password is a leak, so
   * the ambiguity is resolved toward masking. Do not "fix" this to strict RFC parsing. For the
   * same reason a password holding a raw '?' or '#' is not masked whole: they end the user info,
   * because a query such as "?x=a@b" is far more common, and the drivers require them encoded.
   *
   * @param url the connection URL
   * @return the masked URL, or null for null
   */
  public static @Nullable String connectionUrl(final @Nullable String url) {
    if (url == null) {
      return null;
    }
    return replaceControlCharacters(maskSecretPairs(maskUserInfo(url)));
  }

  private static String maskUserInfo(final String url) {
    final int length = url.length();
    StringBuilder sb = null;
    int copied = 0;
    int searchFrom = 0;
    int schemeEnd;
    while ((schemeEnd = url.indexOf("://", searchFrom)) >= 0) {
      final int userInfoStart = schemeEnd + 3;
      int end = userInfoStart;
      int lastAt = -1;
      int firstColon = -1;
      while (end < length && !isUserInfoEnd(url.charAt(end))) {
        final char c = url.charAt(end);
        if (c == '@') {
          lastAt = end;
        } else if (c == ':' && firstColon < 0) {
          firstColon = end;
        }
        end++;
      }
      // Everything up to the last '@' is treated as user info, even past a '/', because a raw '/'
      // or '@' in a password is indistinguishable from a path (see the method comment). The
      // known cost is that "host/db@x" loses its host; that is accepted to never leak a password.
      if (lastAt >= 0) {
        final boolean hasPassword = firstColon >= 0 && firstColon < lastAt;
        if (sb == null) {
          sb = new StringBuilder(length);
        }
        sb.append(url, copied, userInfoStart).append(hasPassword ? MASK + ':' + MASK : MASK);
        copied = lastAt;
      }
      // Anything up to 'end' was scanned; a later "://" inside it is not a new URL.
      searchFrom = Math.max(end, userInfoStart);
    }
    if (sb == null) {
      return url;
    }
    return sb.append(url, copied, length).toString();
  }

  private static boolean isUserInfoEnd(final char c) {
    return c == '?' || c == '#' || Character.isWhitespace(c);
  }

  /**
   * Masks the value of every "name=value" pair whose name contains password, passwd, pwd, secret,
   * token or credential, or is "key". The value ends at whitespace, '&amp;' or a quote. Each '='
   * looks back over its own name only, so the total work is linear.
   */
  private static String maskSecretPairs(final String text) {
    final int length = text.length();
    StringBuilder sb = null;
    int copied = 0;
    for (int sep = text.indexOf('='); sep >= 0; sep = text.indexOf('=', sep + 1)) {
      if (sep < copied) {
        continue;
      }
      int nameStart = sep;
      while (nameStart > copied && isNameCharacter(text.charAt(nameStart - 1))) {
        nameStart--;
      }
      if (nameStart == sep || !isSecretName(text.substring(nameStart, sep))) {
        continue;
      }
      int valueEnd = sep + 1;
      while (valueEnd < length && !isValueEnd(text.charAt(valueEnd))) {
        valueEnd++;
      }
      if (valueEnd == sep + 1) {
        continue;
      }
      if (sb == null) {
        sb = new StringBuilder(length);
      }
      sb.append(text, copied, sep + 1).append(MASK);
      copied = valueEnd;
    }
    if (sb == null) {
      return text;
    }
    return sb.append(text, copied, length).toString();
  }

  private static boolean isSecretName(final String name) {
    final String lower = name.toLowerCase(Locale.ROOT);
    if (lower.equals("key")) {
      return true;
    }
    for (final String secret : SECRET_NAMES) {
      if (lower.contains(secret)) {
        return true;
      }
    }
    return false;
  }

  private static boolean isNameCharacter(final char c) {
    return Character.isLetterOrDigit(c) || c == '_' || c == '-' || c == '.';
  }

  private static boolean isValueEnd(final char c) {
    return c == '&' || c == '"' || c == '\'' || Character.isWhitespace(c);
  }

  /**
   * Summarizes a failure for logs and wrapped exception messages without repeating the raw
   * message. KMS errors such as AccessDenied repeat the key ARN and the caller's role ARN, so for
   * an AWS service error only the exception type, error code and HTTP status are kept. For any
   * other error the type is kept with its message, in which every ARN is masked and control
   * characters are replaced.
   *
   * @param error the failure
   * @return the summary, or null for null
   */
  public static @Nullable String errorSummary(final @Nullable Throwable error) {
    if (error == null) {
      return null;
    }
    final String type = error.getClass().getSimpleName();
    if (error instanceof AwsServiceException) {
      final AwsServiceException awsError = (AwsServiceException) error;
      final AwsErrorDetails details = awsError.awsErrorDetails();
      final String code = details == null ? null : details.errorCode();
      return type + "[errorCode=" + (code == null ? "unknown" : errorMessage(code))
          + ", httpStatus=" + awsError.statusCode() + "]";
    }
    final String message = errorMessage(error.getMessage());
    return message == null || message.isEmpty() ? type : type + ": " + message;
  }

  /**
   * Masks every ARN, URL credential and password-like "name=value" pair in free text, replaces
   * control characters and limits the length. KMS key ARNs are masked with {@link #arn}; any other
   * ARN (such as a role) is replaced entirely.
   *
   * @param message the text
   * @return the sanitized text, or null for null
   */
  public static @Nullable String errorMessage(final @Nullable String message) {
    if (message == null) {
      return null;
    }
    final String cleaned = replaceControlCharacters(maskSecretPairs(maskUserInfo(maskArns(message))));
    return cleaned.length() > MAX_ERROR_MESSAGE_LENGTH
        ? cleaned.substring(0, MAX_ERROR_MESSAGE_LENGTH - 3) + "..."
        : cleaned;
  }

  /**
   * Masks every ARN in the text. A KMS key ARN is masked with {@link #arn}; any other ARN is
   * replaced entirely. An ARN ends at whitespace, a quote, a bracket or '&amp;', ';', '|'.
   */
  private static String maskArns(final String message) {
    final StringBuilder sb = new StringBuilder(message.length());
    int copied = 0;
    int searchFrom = 0;
    int start;
    while ((start = message.indexOf("arn:", searchFrom)) >= 0) {
      // "learn:more" is not an ARN.
      if (start > 0 && Character.isLetterOrDigit(message.charAt(start - 1))) {
        searchFrom = start + 4;
        continue;
      }
      int end = start;
      while (end < message.length() && !isArnEnd(message.charAt(end))) {
        end++;
      }
      // A trailing '.' or ',' is punctuation around the ARN, not part of it.
      while (end > start && (message.charAt(end - 1) == '.' || message.charAt(end - 1) == ',')) {
        end--;
      }
      sb.append(message, copied, start).append(arn(message.substring(start, end)));
      copied = end;
      searchFrom = Math.max(end, start + 4);
    }
    return sb.append(message, copied, message.length()).toString();
  }

  private static boolean isKmsArn(final String token) {
    final String[] parts = token.split(":", 6);
    return parts.length == 6 && "kms".equals(parts[2]);
  }

  private static boolean isArnEnd(final char c) {
    return Character.isWhitespace(c) || c == '"' || c == '\'' || c == '(' || c == ')'
        || c == '[' || c == ']' || c == '{' || c == '}' || c == '<' || c == '>' || c == '&'
        || c == ';' || c == '|' || c == '\\';
  }

  /**
   * Replaces control characters with '?': ISO control characters, line and paragraph separators
   * format characters (such as bidirectional overrides) and unassigned ones, so a value cannot forge or hide log
   * text.
   *
   * @param value the text
   * @return the text without control characters
   */
  public static String replaceControlCharacters(final String value) {
    final StringBuilder sb = new StringBuilder(value.length());
    int i = 0;
    while (i < value.length()) {
      final int codePoint = value.codePointAt(i);
      i += Character.charCount(codePoint);
      final int type = Character.getType(codePoint);
      final boolean control = Character.isISOControl(codePoint)
          || type == Character.LINE_SEPARATOR
          || type == Character.PARAGRAPH_SEPARATOR
          || type == Character.FORMAT
          || type == Character.UNASSIGNED;
      sb.appendCodePoint(control ? '?' : codePoint);
    }
    return sb.toString();
  }
}
