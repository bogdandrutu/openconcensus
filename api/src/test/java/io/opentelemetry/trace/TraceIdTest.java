/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.trace;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link TraceId}. */
class TraceIdTest {
  private static final byte[] firstBytes =
      new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'a'};
  private static final byte[] secondBytes =
      new byte[] {(byte) 0xFF, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'A'};
  private static final String first = TraceId.bytesToHex(firstBytes);

  private static final String second =
      TraceId.fromLongs(
          ByteBuffer.wrap(secondBytes).getLong(), ByteBuffer.wrap(secondBytes, 8, 8).getLong());

  @Test
  void invalidTraceId() {
    assertThat(TraceId.getTraceIdRandomPart(TraceId.getInvalid())).isEqualTo(0);
  }

  @Test
  void isValid() {
    assertThat(TraceId.isValid(TraceId.getInvalid())).isFalse();
    assertThat(TraceId.isValid(first)).isTrue();
    assertThat(TraceId.isValid(second)).isTrue();

    assertThat(TraceId.isValid("000000000000004z0000000000000016")).isFalse();
  }

  @Test
  void testGetRandomTracePart() {
    byte[] id = {
      0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x00
    };
    String traceId = TraceId.bytesToHex(id);
    assertThat(TraceId.getTraceIdRandomPart(traceId)).isEqualTo(0x090A0B0C0D0E0F00L);
  }

  @Test
  void testGetRandomTracePart_NegativeLongRepresentation() {
    byte[] id = {
      0x01,
      0x02,
      0x03,
      0x04,
      0x05,
      0x06,
      0x07,
      0x08,
      (byte) 0xFF, // force a negative value
      0x0A,
      0x0B,
      0x0C,
      0x0D,
      0x0E,
      0x0F,
      0x00
    };
    String traceId = TraceId.bytesToHex(id);
    assertThat(TraceId.getTraceIdRandomPart(traceId)).isEqualTo(0xFF0A0B0C0D0E0F00L);
  }

  @Test
  void fromLowerBase16() {
    assertThat(TraceId.bytesToHex(TraceId.bytesFromHex("00000000000000000000000000000000", 0)))
        .isEqualTo(TraceId.getInvalid());
    assertThat(TraceId.bytesFromHex("00000000000000000000000000000061", 0)).isEqualTo(firstBytes);
    assertThat(TraceId.bytesFromHex("ff000000000000000000000000000041", 0)).isEqualTo(secondBytes);
  }

  @Test
  void fromLowerBase16_WithOffset() {
    assertThat(TraceId.bytesToHex(TraceId.bytesFromHex("XX00000000000000000000000000000000CC", 2)))
        .isEqualTo(TraceId.getInvalid());
    assertThat(TraceId.bytesFromHex("YY00000000000000000000000000000061AA", 2))
        .isEqualTo(firstBytes);
    assertThat(TraceId.bytesFromHex("ZZff000000000000000000000000000041BB", 2))
        .isEqualTo(secondBytes);
  }

  @Test
  public void toLowerBase16() {
    assertThat(TraceId.getInvalid()).isEqualTo("00000000000000000000000000000000");
    assertThat(TraceId.bytesToHex(firstBytes)).isEqualTo("00000000000000000000000000000061");
    assertThat(TraceId.bytesToHex(secondBytes)).isEqualTo("ff000000000000000000000000000041");
  }
}
