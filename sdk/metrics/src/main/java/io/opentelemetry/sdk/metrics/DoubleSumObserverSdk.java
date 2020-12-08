/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.metrics;

import io.opentelemetry.api.metrics.DoubleSumObserver;
import io.opentelemetry.sdk.metrics.AbstractAsynchronousInstrument.AbstractDoubleAsynchronousInstrument;
import io.opentelemetry.sdk.metrics.common.InstrumentType;
import io.opentelemetry.sdk.metrics.common.InstrumentValueType;
import javax.annotation.Nullable;

final class DoubleSumObserverSdk extends AbstractDoubleAsynchronousInstrument
    implements DoubleSumObserver {

  DoubleSumObserverSdk(
      InstrumentDescriptor descriptor,
      MeterProviderSharedState meterProviderSharedState,
      MeterSharedState meterSharedState,
      Batcher batcher,
      @Nullable Callback<DoubleResult> metricUpdater) {
    super(
        descriptor,
        meterProviderSharedState,
        meterSharedState,
        new ActiveBatcher(batcher),
        metricUpdater);
  }

  static final class Builder
      extends AbstractAsynchronousInstrument.Builder<DoubleSumObserverSdk.Builder>
      implements DoubleSumObserver.Builder {

    @Nullable private Callback<DoubleResult> callback;

    Builder(
        String name,
        MeterProviderSharedState meterProviderSharedState,
        MeterSharedState meterSharedState,
        MeterSdk meterSdk) {
      super(name, meterProviderSharedState, meterSharedState, meterSdk);
    }

    @Override
    Builder getThis() {
      return this;
    }

    @Override
    public Builder setCallback(Callback<DoubleResult> callback) {
      this.callback = callback;
      return this;
    }

    @Override
    public DoubleSumObserverSdk build() {
      InstrumentDescriptor instrumentDescriptor =
          getInstrumentDescriptor(InstrumentType.SUM_OBSERVER, InstrumentValueType.DOUBLE);
      DoubleSumObserverSdk instrument =
          new DoubleSumObserverSdk(
              instrumentDescriptor,
              getMeterProviderSharedState(),
              getMeterSharedState(),
              getBatcher(instrumentDescriptor),
              callback);
      return register(instrument);
    }
  }
}
