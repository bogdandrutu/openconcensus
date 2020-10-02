/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.internal;

import io.opentelemetry.common.AttributeKey;
import io.opentelemetry.common.AttributeType;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/** Internal utility methods for working with attribute keys, attribute values, and metric names. */
@Immutable
public final class StringUtils {

  public static final int METRIC_NAME_MAX_LENGTH = 255;

  /**
   * Determines whether the {@code String} contains only printable characters.
   *
   * @param str the {@code String} to be validated.
   * @return whether the {@code String} contains only printable characters.
   */
  public static boolean isPrintableString(String str) {
    for (int i = 0; i < str.length(); i++) {
      if (!isPrintableChar(str.charAt(i))) {
        return false;
      }
    }
    return true;
  }

  private static boolean isPrintableChar(char ch) {
    return ch >= ' ' && ch <= '~';
  }

  /**
   * Determines whether the metric name contains a valid metric name.
   *
   * @param metricName the metric name to be validated.
   * @return whether the metricName contains a valid name.
   */
  public static boolean isValidMetricName(String metricName) {
    if (metricName.isEmpty() || metricName.length() > METRIC_NAME_MAX_LENGTH) {
      return false;
    }
    String pattern = "[aA-zZ][aA-zZ0-9_\\-.]*";
    return metricName.matches(pattern);
  }

  /**
   * If given attribute is of type STRING and has more characters than given {@code limit} then
   * return new value with string truncated to {@code limit} characters.
   *
   * <p>If given attribute is of type STRING_ARRAY and non-empty then return new value with every
   * element truncated to {@code limit} characters.
   *
   * <p>Otherwise return given {@code value}
   *
   * @throws IllegalArgumentException if limit is zero or negative
   */
  @SuppressWarnings("unchecked")
  public static <T> T truncateToSize(AttributeKey<T> key, T value, int limit) {
    Utils.checkArgument(limit > 0, "attribute value limit must be positive, got %d", limit);

    if (value == null
        || ((key.getType() != AttributeType.STRING)
            && (key.getType() != AttributeType.STRING_ARRAY))) {
      return value;
    }

    if (key.getType() == AttributeType.STRING_ARRAY) {
      List<String> strings = (List<String>) value;
      if (strings.isEmpty()) {
        return value;
      }

      List<String> newStrings = new ArrayList<>(strings.size());
      for (String string : strings) {
        newStrings.add(truncateToSize(string, limit));
      }

      return (T) newStrings;
    }

    return (T) truncateToSize((String) value, limit);
  }

  @Nullable
  private static String truncateToSize(@Nullable String s, int limit) {
    if (s == null || s.length() <= limit) {
      return s;
    }
    return s.substring(0, limit);
  }

  private StringUtils() {}
}
