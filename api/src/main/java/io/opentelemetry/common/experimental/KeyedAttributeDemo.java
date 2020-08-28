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

package io.opentelemetry.common.experimental;

import io.opentelemetry.common.experimental.KeyedAttributes.BooleanArrayKey;
import io.opentelemetry.common.experimental.KeyedAttributes.BooleanKey;
import io.opentelemetry.common.experimental.KeyedAttributes.KeyImpl;
import io.opentelemetry.common.experimental.KeyedAttributes.StringArrayKey;
import io.opentelemetry.common.experimental.KeyedAttributes.StringKey;
import io.opentelemetry.common.experimental.ReadableKeyedAttributes.AttributeConsumer;
import java.math.BigDecimal;
import java.util.List;

@SuppressWarnings("JavadocMethod")
public class KeyedAttributeDemo {
  private KeyedAttributeDemo() {}

  // here are some semantic attributes examples:
  static final StringKey HTTP_METHOD = KeyedAttributes.stringKey("http.method");
  static final StringKey NET_HOST_NAME = KeyedAttributes.stringKey("net.host.name");
  static final BooleanKey ERROR_HINT = KeyedAttributes.booleanKey("error.hint");
  static final StringArrayKey HTTP_HEADERS = KeyedAttributes.stringArrayKey("http.headers");

  static final KeyedAttributes.Key<BigDecimal> MONEY_VALUE =
      new KeyImpl<BigDecimal>("money.value") {};

  public static void main(String[] args) {
    // here's how you build a full set of attributes. You can imagine the Span/Builder would look
    // similar
    KeyedAttributes keyedAttributes =
        KeyedAttributes.newBuilder()
            .set(HTTP_HEADERS, "x-stuff", "x-api-key")
            .set(HTTP_METHOD, "PUT")
            .set(ERROR_HINT, false)
            .set(NET_HOST_NAME, "localhost")
            .setCustom(MONEY_VALUE, new BigDecimal(1_000_000L))
            .build();

    // iterate over them like this, using the readable interface.
    keyedAttributes.forEach(
        new AttributeConsumer() {
          @Override
          public void consume(StringKey key, String value) {
            System.out.println(key + " = " + value);
          }

          @Override
          public void consume(StringArrayKey key, List<String> value) {
            System.out.println(key + " = " + value);
          }

          @Override
          public void consume(BooleanKey key, boolean value) {
            System.out.println(key + " = " + value);
          }

          @Override
          public void consume(BooleanArrayKey key, List<Boolean> value) {
            System.out.println(key + " = " + value);
          }

          @Override
          public <T> void consumeCustom(KeyedAttributes.Key<T> key, T value) {
            System.out.println(key + " = " + value);
          }
        });
  }
}
