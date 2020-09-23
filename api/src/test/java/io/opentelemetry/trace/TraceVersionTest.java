/*
 * Copyright 2020, OpenTelemetry Authors
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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class TraceVersionTest {

  @Test
  public void isValid() {
    assertTrue(TraceVersion.isValid("00"));
    assertTrue(TraceVersion.isValid("cc"));

    assertFalse(TraceVersion.isValid("ff"));
    assertFalse(TraceVersion.isValid("000"));
    assertFalse(TraceVersion.isValid("0"));
    assertFalse(TraceVersion.isValid("--"));
  }
}
