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

import io.opentelemetry.metrics.LabelSet;
import io.opentelemetry.metrics.LongMeasure;
import java.util.List;
import java.util.Map;

final class LongMeasureSdk extends AbstractInstrument implements LongMeasure {

  private final boolean absolute;

  private LongMeasureSdk(
      String name,
      String description,
      String unit,
      Map<String, String> constantLabels,
      List<String> labelKeys,
      MeterSharedState sharedState,
      boolean absolute) {
    super(name, description, unit, constantLabels, labelKeys, sharedState);
    this.absolute = absolute;
  }

  @Override
  public void record(long value, LabelSet labelSet) {
    BoundInstrument boundInstrument = bind(labelSet);
    boundInstrument.record(value);
    boundInstrument.unbind();
  }

  @Override
  public BoundInstrument bind(LabelSet labelSet) {
    return new BoundInstrument(this.absolute);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LongMeasureSdk)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    LongMeasureSdk that = (LongMeasureSdk) o;

    return absolute == that.absolute;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (absolute ? 1 : 0);
    return result;
  }

  static final class BoundInstrument extends AbstractBoundInstrument implements BoundLongMeasure {

    private final boolean absolute;

    BoundInstrument(boolean absolute) {
      super(null);
      this.absolute = absolute;
    }

    @Override
    public void record(long value) {
      if (this.absolute && value < 0) {
        throw new IllegalArgumentException("absolute measure can only record positive values");
      }
      // TODO: pass through to an aggregator/accumulator
    }
  }

  static LongMeasure.Builder builder(String name, MeterSharedState sharedState) {
    return new Builder(name, sharedState);
  }

  private static final class Builder
      extends AbstractMeasureBuilder<LongMeasure.Builder, LongMeasure>
      implements LongMeasure.Builder {

    private Builder(String name, MeterSharedState sharedState) {
      super(name, sharedState);
    }

    @Override
    Builder getThis() {
      return this;
    }

    @Override
    public LongMeasure build() {
      return new LongMeasureSdk(
          getName(),
          getDescription(),
          getUnit(),
          getConstantLabels(),
          getLabelKeys(),
          getMeterSharedState(),
          isAbsolute());
    }
  }
}
