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

package io.opentelemetry.opentracingshim;

import io.opentelemetry.context.propagation.BinaryFormat;
import io.opentelemetry.context.propagation.HttpTextFormat;
import io.opentracing.SpanContext;
import io.opentracing.propagation.Binary;
import io.opentracing.propagation.TextMap;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

final class Propagation {
  private Propagation() {}

  public static void injectTextFormat(
      HttpTextFormat<io.opentelemetry.trace.SpanContext> format,
      io.opentelemetry.trace.SpanContext context,
      TextMap carrier) {
    format.inject(context, carrier, TextMapSetter.INSTANCE);
  }

  public static SpanContext extractTextFormat(
      HttpTextFormat<io.opentelemetry.trace.SpanContext> format, TextMap carrier) {
    Map<String, String> carrierMap = new HashMap<String, String>();
    for (Map.Entry<String, String> entry : carrier) {
      carrierMap.put(entry.getKey(), entry.getValue());
    }

    return new SpanContextShim(format.extract(carrierMap, TextMapGetter.INSTANCE));
  }

  static final class TextMapSetter implements HttpTextFormat.Setter<TextMap> {
    private TextMapSetter() {}

    public static final TextMapSetter INSTANCE = new TextMapSetter();

    @Override
    public void put(TextMap carrier, String key, String value) {
      carrier.put(key, value);
    }
  }

  // We use Map<> instead of TextMap as we need to query a specified key, and iterating over
  // *all* values per key-query *might* be a bad idea.
  static final class TextMapGetter implements HttpTextFormat.Getter<Map<String, String>> {
    private TextMapGetter() {}

    public static final TextMapGetter INSTANCE = new TextMapGetter();

    @Override
    public String get(Map<String, String> carrier, String key) {
      return carrier.get(key);
    }
  }

  public static void injectBinaryFormat(
      BinaryFormat<io.opentelemetry.trace.SpanContext> format,
      io.opentelemetry.trace.SpanContext context,
      Binary carrier) {

    byte[] buff = format.toByteArray(context);
    ByteBuffer byteBuff = carrier.injectionBuffer(buff.length);
    byteBuff.put(buff);
  }

  public static SpanContext extractBinaryFormat(
      BinaryFormat<io.opentelemetry.trace.SpanContext> format, Binary carrier) {

    ByteBuffer byteBuff = carrier.extractionBuffer();
    byte[] buff = new byte[byteBuff.remaining()];
    byteBuff.get(buff);

    return new SpanContextShim(format.fromByteArray(buff));
  }
}
