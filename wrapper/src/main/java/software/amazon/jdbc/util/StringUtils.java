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

package software.amazon.jdbc.util;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.checkerframework.checker.nullness.qual.EnsuresNonNullIf;
import org.checkerframework.checker.nullness.qual.Nullable;

public class StringUtils {

  private static final String ENCODING_FOR_URL =
      System.getProperty("software.amazon.jdbc.Driver.url.encoding", "UTF-8");

  public static String decode(final String encoded) {
    try {
      return URLDecoder.decode(encoded, ENCODING_FOR_URL);
    } catch (final UnsupportedEncodingException e) {
      throw new IllegalStateException(
          "Unable to decode URL entry via " + ENCODING_FOR_URL + ". This should not happen", e);
    }
  }

  public static String encode(final String plain) {
    try {
      return URLEncoder.encode(plain, "UTF-8");
    } catch (final UnsupportedEncodingException e) {
      throw new IllegalStateException(
          "Unable to encode URL entry via " + ENCODING_FOR_URL + ". This should not happen", e);
    }
  }

  /**
   * Check if the supplied string is null or empty.
   *
   * @param s the string to analyze
   * @return true if the supplied string is null or empty
   */
  @EnsuresNonNullIf(expression = "#1", result = false)
  public static boolean isNullOrEmpty(@Nullable final String s) {
    return s == null || s.equals("");
  }

  /**
   * Splits stringToSplit into a list, using the given delimiter.
   *
   * @param stringToSplit the string to split
   * @param delimiter     the string to split on
   * @param trim          should the split strings be whitespace trimmed?
   * @return the list of strings, split by delimiter
   * @throws IllegalArgumentException if an error occurs
   */
  public static List<String> split(final String stringToSplit, final String delimiter, final boolean trim) {
    if (stringToSplit == null || "".equals(stringToSplit)) {
      return new ArrayList<>();
    }

    if (delimiter == null) {
      throw new IllegalArgumentException();
    }

    final String[] tokens = stringToSplit.split(delimiter, -1);
    Stream<String> tokensStream = Arrays.stream(tokens);
    if (trim) {
      tokensStream = tokensStream.map(String::trim);
    }
    return tokensStream.filter((x) -> x != null && !"".equals(x)).collect(Collectors.toList());
  }
}
