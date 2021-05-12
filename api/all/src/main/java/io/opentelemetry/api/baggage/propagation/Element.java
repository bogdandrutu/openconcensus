/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.api.baggage.propagation;

import java.util.BitSet;

/**
 * Represents single element of a W3C baggage header (key or value). Allows tracking parsing of a
 * header string, keeping the state and validating allowed characters. Parsing state can be reset
 * with {@link #reset(int)} allowing instance re-use.
 */
class Element {

  private final BitSet excluded = new BitSet(128);

  private boolean leadingSpace;
  private boolean readingValue;
  private boolean trailingSpace;
  private int start;
  private int end;
  private String value;

  /**
   * Constructs element instance.
   *
   * @param excludedChars characters that are not allowed for this type of an element
   */
  Element(char[] excludedChars) {
    for (char excludedChar : excludedChars) {
      excluded.set(excludedChar);
    }
    reset(0);
  }

  String getValue() {
    return value;
  }

  void reset(int start) {
    this.start = start;
    leadingSpace = true;
    readingValue = false;
    trailingSpace = false;
    value = null;
  }

  boolean tryTerminating(int index, String header) {
    if (this.readingValue) {
      markEnd(index);
    }
    if (this.trailingSpace) {
      setValue(header);
      return true;
    } else {
      // leading spaces - no content, invalid
      return false;
    }
  }

  private void markEnd(int end) {
    this.end = end;
    this.readingValue = false;
    trailingSpace = true;
  }

  private void setValue(String header) {
    this.value = header.substring(this.start, this.end);
  }

  boolean tryNextChar(char character, int index) {
    if (isWhitespace(character)) {
      return tryNextWhitespace(index);
    } else if (isExcluded(character)) {
      return false;
    } else {
      return tryNextTokenChar(index);
    }
  }

  private static boolean isWhitespace(char character) {
    return character == ' ' || character == '\t';
  }

  private boolean tryNextWhitespace(int index) {
    if (readingValue) {
      markEnd(index);
    }
    return true;
  }

  private boolean isExcluded(char character) {
    return (character <= 32 || character >= 127 || excluded.get(character));
  }

  private boolean tryNextTokenChar(int index) {
    if (leadingSpace) {
      markStart(index);
    }
    return !trailingSpace;
  }

  private void markStart(int start) {
    this.start = start;
    readingValue = true;
    leadingSpace = false;
  }
}
