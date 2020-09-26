/*
 * Copyright 2019, OpenTelemetry Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.opentelemetry.trace;

import javax.annotation.concurrent.Immutable;

/**
 * Helper methods for dealing with trace flags options. These options are propagated to all child
 * {@link Span spans}. These determine features such as whether a {@code Span} should be traced. It
 * is implemented as a bitmask.
 *
 * @since 0.1.0
 */
@Immutable
public final class TraceFlags {
  private TraceFlags() {}

  // Bit to represent whether trace is sampled or not.
  private static final byte IS_SAMPLED = 0x1;
  // the default flags are a 0 byte.
  private static final byte DEFAULT = 0x0;

  private static final int SIZE = 1;
  private static final int BASE16_SIZE = 2 * SIZE;

  /**
   * Returns the size in Hex of trace flags.
   *
   * @since 0.9.0
   */
  public static int getHexLength() {
    return BASE16_SIZE;
  }

  /**
   * Returns the default {@code TraceFlags}.
   *
   * @return the default {@code TraceFlags}.
   * @since 0.1.0
   */
  public static byte getDefault() {
    return DEFAULT;
  }

  /** Extract the sampled flag from hex-based trace-flags. */
  public static boolean isSampledFromHex(CharSequence src, int srcOffset) {
    // todo bypass the byte conversion and look directly at the hex.
    byte b = BigendianEncoding.byteFromBase16String(src, srcOffset);
    return isSampled(b);
  }

  /** Extract the sampled flag from trace-flags. */
  public static boolean isSampled(byte traceFlags) {
    return (traceFlags & IS_SAMPLED) != 0;
  }

  /** Extract the byte representation of the flags from a hex-representation. */
  public static byte byteFromHex(CharSequence src, int srcOffset) {
    return BigendianEncoding.byteFromBase16String(src, srcOffset);
  }

  public static byte getSampled() {
    return IS_SAMPLED;
  }
}
