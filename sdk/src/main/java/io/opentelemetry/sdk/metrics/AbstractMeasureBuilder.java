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

package io.opentelemetry.sdk.metrics;

import io.opentelemetry.metrics.Measure;

abstract class AbstractMeasureBuilder<B extends Measure.Builder<B, V>, V>
    extends AbstractInstrumentBuilder<B, V> implements Measure.Builder<B, V> {
  private boolean absolute = true;

  protected AbstractMeasureBuilder(String name) {
    super(name);
  }

  @Override
  public final B setAbsolute(boolean absolute) {
    this.absolute = absolute;
    return getThis();
  }

  final boolean getAbsolute() {
    return this.absolute;
  }
}
