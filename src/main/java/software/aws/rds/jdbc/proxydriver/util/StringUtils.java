/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.util;

import org.checkerframework.checker.nullness.qual.EnsuresNonNullIf;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;

public class StringUtils {

    private static final String ENCODING_FOR_URL =
            System.getProperty("software.aws.rds.jdbc.proxydriver.Driver.url.encoding", "UTF-8");

    public static String decode(String encoded) {
        try {
            return URLDecoder.decode(encoded, ENCODING_FOR_URL);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(
                    "Unable to decode URL entry via " + ENCODING_FOR_URL + ". This should not happen", e);
        }
    }

    public static String encode(String plain) {
        try {
            return URLEncoder.encode(plain, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(
                    "Unable to encode URL entry via " + ENCODING_FOR_URL + ". This should not happen", e);
        }
    }

    /**
     * Check if the supplied string is null or empty
     *
     * @param s the string to analyze
     * @return true if the supplied string is null or empty
     */
    @EnsuresNonNullIf(expression = "#1", result = false)
    public static boolean isNullOrEmpty(@Nullable String s) {
        return s == null || s.equals("");
    }

}
