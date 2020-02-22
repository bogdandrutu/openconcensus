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

package io.opentelemetry.sdk.metrics;

import io.opentelemetry.metrics.BatchRecorder;
import io.opentelemetry.metrics.LabelSet;
import io.opentelemetry.metrics.Meter;
import io.opentelemetry.sdk.common.InstrumentationLibraryInfo;
import java.util.Map;

/** {@link MeterSdk} is SDK implementation of {@link Meter}. */
final class MeterSdk implements Meter {
  private final MeterProviderSharedState meterProviderSharedState;
  private final MeterSharedState meterSharedState;

  MeterSdk(
      MeterProviderSharedState meterProviderSharedState,
      InstrumentationLibraryInfo instrumentationLibraryInfo) {
    this.meterProviderSharedState = meterProviderSharedState;
    this.meterSharedState = MeterSharedState.create(instrumentationLibraryInfo);
  }

  InstrumentationLibraryInfo getInstrumentationLibraryInfo() {
    return meterSharedState.getInstrumentationLibraryInfo();
  }

  @Override
  public DoubleCounterSdk.Builder doubleCounterBuilder(String name) {
    return new DoubleCounterSdk.Builder(name, meterProviderSharedState, meterSharedState);
  }

  @Override
  public LongCounterSdk.Builder longCounterBuilder(String name) {
    return new LongCounterSdk.Builder(name, meterProviderSharedState, meterSharedState);
  }

  @Override
  public DoubleMeasureSdk.Builder doubleMeasureBuilder(String name) {
    return new DoubleMeasureSdk.Builder(name, meterProviderSharedState, meterSharedState);
  }

  @Override
  public LongMeasureSdk.Builder longMeasureBuilder(String name) {
    return new LongMeasureSdk.Builder(name, meterProviderSharedState, meterSharedState);
  }

  @Override
  public DoubleObserverSdk.Builder doubleObserverBuilder(String name) {
    return new DoubleObserverSdk.Builder(name, meterProviderSharedState, meterSharedState);
  }

  @Override
  public LongObserverSdk.Builder longObserverBuilder(String name) {
    return new LongObserverSdk.Builder(name, meterProviderSharedState, meterSharedState);
  }

  @Override
  public BatchRecorder newBatchRecorder(LabelSet labelSet) {
    throw new UnsupportedOperationException("to be implemented");
  }

  @Override
  public LabelSetSdk createLabelSet(String... keyValuePairs) {
    return LabelSetSdk.create(keyValuePairs);
  }

  @Override
  public LabelSetSdk createLabelSet(Map<String, String> labels) {
    return LabelSetSdk.create(labels);
  }
}
