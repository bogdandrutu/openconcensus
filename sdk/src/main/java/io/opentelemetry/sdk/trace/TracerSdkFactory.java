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

package io.opentelemetry.sdk.trace;

import io.opentelemetry.trace.Tracer;
import io.opentelemetry.trace.TracerFactory;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@code Tracer} provider implementation for {@link TracerFactory}.
 *
 * <p>This class is not intended to be used in application code and it is used only by {@link
 * io.opentelemetry.OpenTelemetry}.
 */
public class TracerSdkFactory implements TracerFactory {
  private final Map<String, Tracer> tracersByKey = new ConcurrentHashMap<>();

  @Override
  public Tracer get(String instrumentationName) {
    return get(instrumentationName, null);
  }

  @Override
  public Tracer get(String instrumentationName, String instrumentationVersion) {
    String key = instrumentationName + "/" + instrumentationVersion;
    Tracer tracer = tracersByKey.get(key);
    if (tracer == null) {
      // todo: pass in the name & version here to the implementation to be used for purposes.
      tracer = new TracerSdk();
      tracersByKey.put(key, tracer);
    }
    return tracer;
  }
}
