/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.api.trace;

import java.util.Objects;
import javax.annotation.concurrent.Immutable;

/**
 * Helper methods for dealing with a span identifier represented as lowercase hex Strings. A valid
 * span identifier is a 16 character lowercase hex (base16) String, where at least one of the
 * characters is not a "0".
 *
 * <p>There are two more other representation that this class helps with:
 *
 * <ul>
 *   <li>Long: a {@code long} value, where valid means that the value is non-zero.
 * </ul>
 */
@Immutable
public final class SpanIdHex {
  private static final ThreadLocal<char[]> charBuffer = new ThreadLocal<>();

  private static final int HEX_LENGTH = 16;
  private static final String INVALID = "0000000000000000";

  private SpanIdHex() {}

  /**
   * Returns the length of the lowercase hex (base16) representation of the {@code SpanId}.
   *
   * @return the length of the lowercase hex (base16) representation of the {@code SpanId}.
   */
  public static int getLength() {
    return HEX_LENGTH;
  }

  /**
   * Returns the invalid {@code SpanId} in lowercase hex (base16) representation. All characters are
   * "0".
   *
   * @return the invalid {@code SpanId} lowercase in hex (base16) representation.
   */
  public static String getInvalid() {
    return INVALID;
  }

  /**
   * Returns whether the span identifier is valid. A valid span identifier is a 16 character hex
   * String, where at least one of the characters is not a '0'.
   *
   * @return {@code true} if the span identifier is valid.
   */
  public static boolean isValid(String spanIdHex) {
    return (spanIdHex.length() == HEX_LENGTH)
        && !INVALID.equals(spanIdHex)
        && BigendianEncoding.isValidBase16String(spanIdHex);
  }

  /**
   * Returns the bytes (8-byte array) representation of the {@code SpanId} converted from the given
   * lowercase hex (base16) representation.
   *
   * @param spanId the lowercase hex (base16) representation of the {@code SpanId}.
   * @return the bytes (8-byte array) representation of the {@code SpanId}.
   * @throws NullPointerException if {@code spanId} is null.
   * @throws IndexOutOfBoundsException if {@code spanId} too short.
   * @throws IllegalArgumentException if {@code spanId} contains non lowercase hex characters.
   */
  static byte[] asBytes(CharSequence spanId) {
    Objects.requireNonNull(spanId, "spanId");
    return BigendianEncoding.bytesFromBase16(spanId, HEX_LENGTH);
  }

  /**
   * Returns the lowercase hex (base16) representation of the {@code SpanId} converted from the
   * given {@code long} value representation.
   *
   * <p>There is no restriction on the specified values, other than the already established validity
   * rules applying to {@code SpanId}. Specifying 0 for the long value will effectively return
   * {@link #getInvalid()}.
   *
   * @param id the higher part of the {@code TraceId}.
   * @return the lowercase hex (base16) representation of the {@code SpanId}.
   */
  public static String fromLong(long id) {
    if (id == 0) {
      return getInvalid();
    }
    char[] result = getTemporaryBuffer();
    BigendianEncoding.longToBase16String(id, result, 0);
    return new String(result);
  }

  private static char[] getTemporaryBuffer() {
    char[] chars = charBuffer.get();
    if (chars == null) {
      chars = new char[HEX_LENGTH];
      charBuffer.set(chars);
    }
    return chars;
  }
}