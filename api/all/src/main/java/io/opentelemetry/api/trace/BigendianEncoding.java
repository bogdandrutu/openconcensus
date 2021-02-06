/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.api.trace;

import io.opentelemetry.api.internal.Utils;
import java.util.Arrays;
import javax.annotation.concurrent.Immutable;

@Immutable
final class BigendianEncoding {
  static final int LONG_BYTES = Long.SIZE / Byte.SIZE;
  static final int BYTE_BASE16 = 2;
  static final int LONG_BASE16 = BYTE_BASE16 * LONG_BYTES;
  private static final String ALPHABET = "0123456789abcdef";
  private static final int ASCII_CHARACTERS = 128;
  private static final char[] ENCODING = buildEncodingArray();
  private static final byte[] DECODING = buildDecodingArray();

  private static char[] buildEncodingArray() {
    char[] encoding = new char[512];
    for (int i = 0; i < 256; ++i) {
      encoding[i] = ALPHABET.charAt(i >>> 4);
      encoding[i | 0x100] = ALPHABET.charAt(i & 0xF);
    }
    return encoding;
  }

  private static byte[] buildDecodingArray() {
    byte[] decoding = new byte[ASCII_CHARACTERS];
    Arrays.fill(decoding, (byte) -1);
    for (int i = 0; i < ALPHABET.length(); i++) {
      char c = ALPHABET.charAt(i);
      decoding[c] = (byte) i;
    }
    return decoding;
  }

  /**
   * Returns the {@code long} value whose base16 representation is stored in the first 16 chars of
   * {@code chars} starting from the {@code offset}.
   *
   * @param chars the base16 representation of the {@code long}.
   * @param offset the starting offset in the {@code CharSequence}.
   */
  static long longFromBase16String(CharSequence chars, int offset) {
    return (byteFromBase16(chars.charAt(offset), chars.charAt(offset + 1)) & 0xFFL) << 56
        | (byteFromBase16(chars.charAt(offset + 2), chars.charAt(offset + 3)) & 0xFFL) << 48
        | (byteFromBase16(chars.charAt(offset + 4), chars.charAt(offset + 5)) & 0xFFL) << 40
        | (byteFromBase16(chars.charAt(offset + 6), chars.charAt(offset + 7)) & 0xFFL) << 32
        | (byteFromBase16(chars.charAt(offset + 8), chars.charAt(offset + 9)) & 0xFFL) << 24
        | (byteFromBase16(chars.charAt(offset + 10), chars.charAt(offset + 11)) & 0xFFL) << 16
        | (byteFromBase16(chars.charAt(offset + 12), chars.charAt(offset + 13)) & 0xFFL) << 8
        | (byteFromBase16(chars.charAt(offset + 14), chars.charAt(offset + 15)) & 0xFFL);
  }

  /**
   * Appends the base16 encoding of the specified {@code value} to the {@code dest}.
   *
   * @param value the value to be converted.
   * @param dest the destination char array.
   * @param destOffset the starting offset in the destination char array.
   */
  static void longToBase16String(long value, char[] dest, int destOffset) {
    byteToBase16((byte) (value >> 56 & 0xFFL), dest, destOffset);
    byteToBase16((byte) (value >> 48 & 0xFFL), dest, destOffset + BYTE_BASE16);
    byteToBase16((byte) (value >> 40 & 0xFFL), dest, destOffset + 2 * BYTE_BASE16);
    byteToBase16((byte) (value >> 32 & 0xFFL), dest, destOffset + 3 * BYTE_BASE16);
    byteToBase16((byte) (value >> 24 & 0xFFL), dest, destOffset + 4 * BYTE_BASE16);
    byteToBase16((byte) (value >> 16 & 0xFFL), dest, destOffset + 5 * BYTE_BASE16);
    byteToBase16((byte) (value >> 8 & 0xFFL), dest, destOffset + 6 * BYTE_BASE16);
    byteToBase16((byte) (value & 0xFFL), dest, destOffset + 7 * BYTE_BASE16);
  }

  static byte[] bytesFromBase16(CharSequence value, int length) {
    byte[] result = new byte[length / 2];
    for (int i = 0; i < length; i += 2) {
      result[i / 2] = byteFromBase16(value.charAt(i), value.charAt(i + 1));
    }
    return result;
  }

  /**
   * Encodes the specified byte, and returns the encoded {@code String}.
   *
   * @param value the value to be converted.
   * @param dest the destination char array.
   * @param destOffset the starting offset in the destination char array.
   */
  static void byteToBase16(byte value, char[] dest, int destOffset) {
    int b = value & 0xFF;
    dest[destOffset] = ENCODING[b];
    dest[destOffset + 1] = ENCODING[b | 0x100];
  }

  /**
   * Decodes the specified two character sequence, and returns the resulting {@code byte}.
   *
   * @param first the first hex character.
   * @param second the second hex character.
   * @return the resulting {@code byte}
   */
  static byte byteFromBase16(char first, char second) {
    Utils.checkArgument(
        first < ASCII_CHARACTERS && DECODING[first] != -1, "invalid character " + first);
    Utils.checkArgument(
        second < ASCII_CHARACTERS && DECODING[second] != -1, "invalid character " + second);
    int decoded = DECODING[first] << 4 | DECODING[second];
    return (byte) decoded;
  }

  static boolean isValidBase16String(CharSequence value) {
    for (int i = 0; i < value.length(); i++) {
      char b = value.charAt(i);
      // 48..57 && 97..102 are valid
      if (!isDigit(b) && !isLowercaseHexCharacter(b)) {
        return false;
      }
    }
    return true;
  }

  private static boolean isLowercaseHexCharacter(char b) {
    return 97 <= b && b <= 102;
  }

  private static boolean isDigit(char b) {
    return 48 <= b && b <= 57;
  }

  private BigendianEncoding() {}
}
