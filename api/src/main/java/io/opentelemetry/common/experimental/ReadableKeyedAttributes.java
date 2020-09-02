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

import io.opentelemetry.common.AttributeValue;
import io.opentelemetry.common.experimental.KeyedAttributes.BooleanArrayKey;
import io.opentelemetry.common.experimental.KeyedAttributes.BooleanKey;
import io.opentelemetry.common.experimental.KeyedAttributes.CompoundKey;
import io.opentelemetry.common.experimental.KeyedAttributes.MultiAttribute;
import io.opentelemetry.common.experimental.KeyedAttributes.StringArrayKey;
import io.opentelemetry.common.experimental.KeyedAttributes.StringKey;
import java.util.List;

interface ReadableKeyedAttributes {
  void forEach(AttributeConsumer attributeConsumer);

  void forEachRaw(RawAttributeConsumer rawAttributeConsumer);

  interface RawAttributeConsumer {
    <T> void consume(KeyedAttributes.Key<T> key, AttributeValue.Type type, T value);
  }

  interface AttributeConsumer {
    void consume(StringKey key, String value);

    void consume(BooleanKey key, boolean value);

    void consume(StringArrayKey key, List<String> value);

    void consume(BooleanArrayKey key, List<Boolean> value);

    void consume(CompoundKey key, MultiAttribute value);

    <T> void consumeCustom(KeyedAttributes.Key<T> key, T value);
  }
}
