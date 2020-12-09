package io.opentelemetry.api;

import static java.util.Objects.requireNonNull;

import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.spi.metrics.MeterProviderFactory;
import io.opentelemetry.spi.trace.TracerProviderFactory;

/** A builder for DefaultOpenTelemetry. */
public class DefaultOpenTelemetryBuilder {
  protected ContextPropagators propagators = ContextPropagators.noop();
  protected TracerProvider tracerProvider;
  protected MeterProvider meterProvider;

  /** Sets the {@link TracerProvider} to use. */
  public DefaultOpenTelemetryBuilder setTracerProvider(TracerProvider tracerProvider) {
    requireNonNull(tracerProvider, "tracerProvider");
    this.tracerProvider = tracerProvider;
    return this;
  }

  /** Sets the {@link MeterProvider} to use. */
  public DefaultOpenTelemetryBuilder setMeterProvider(MeterProvider meterProvider) {
    requireNonNull(meterProvider, "meterProvider");
    this.meterProvider = meterProvider;
    return this;
  }

  /** Sets the {@link ContextPropagators} to use. */
  public DefaultOpenTelemetryBuilder setPropagators(ContextPropagators propagators) {
    requireNonNull(propagators, "propagators");
    this.propagators = propagators;
    return this;
  }

  /**
   * Returns a new {@link OpenTelemetry} based on the configuration in this {@link
   * DefaultOpenTelemetryBuilder}.
   */
  public OpenTelemetry build() {
    MeterProvider meterProvider = this.meterProvider;
    if (meterProvider == null) {
      MeterProviderFactory meterProviderFactory = Utils.loadSpi(MeterProviderFactory.class);
      if (meterProviderFactory != null) {
        meterProvider = meterProviderFactory.create();
      } else {
        meterProvider = MeterProvider.getDefault();
      }
    }

    TracerProvider tracerProvider = this.tracerProvider;
    if (tracerProvider == null) {
      TracerProviderFactory tracerProviderFactory = Utils.loadSpi(TracerProviderFactory.class);
      if (tracerProviderFactory != null) {
        tracerProvider = tracerProviderFactory.create();
      } else {
        tracerProvider = TracerProvider.getDefault();
      }
    }

    return new DefaultOpenTelemetry(tracerProvider, meterProvider, propagators);
  }
}
