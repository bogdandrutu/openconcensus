/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.api.metrics;

import io.opentelemetry.spi.metrics.MeterProviderFactory;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * IMPORTANT: This is a temporary class, and solution for the metrics package until it will be
 * marked as stable.
 */
public class GlobalMetricsProvider {
  private static final Object mutex = new Object();
  private static final AtomicReference<MeterProvider> globalMeterProvider = new AtomicReference<>();

  private GlobalMetricsProvider() {}

  /** Returns the globally registered {@link MeterProvider}. */
  public static MeterProvider get() {
    MeterProvider meterProvider = globalMeterProvider.get();
    if (meterProvider == null) {
      synchronized (mutex) {
        if (globalMeterProvider.get() == null) {
          MeterProviderFactory meterProviderFactory = loadSpi();
          if (meterProviderFactory != null) {
            meterProvider = meterProviderFactory.create();
          } else {
            meterProvider = MeterProvider.getDefault();
          }
          globalMeterProvider.compareAndSet(null, meterProvider);
        }
      }
    }
    return meterProvider;
  }

  /**
   * Gets or creates a named meter instance from the globally registered {@link MeterProvider}.
   *
   * <p>This is a shortcut method for {@code getGlobalMeterProvider().get(instrumentationName)}
   *
   * @param instrumentationName The name of the instrumentation library, not the name of the
   *     instrument*ed* library.
   * @return a tracer instance.
   */
  public static Meter getMeter(String instrumentationName) {
    return get().get(instrumentationName);
  }

  /**
   * Gets or creates a named and versioned meter instance from the globally registered {@link
   * MeterProvider}.
   *
   * <p>This is a shortcut method for {@code getGlobalMeterProvider().get(instrumentationName,
   * instrumentationVersion)}
   *
   * @param instrumentationName The name of the instrumentation library, not the name of the
   *     instrument*ed* library.
   * @param instrumentationVersion The version of the instrumentation library.
   * @return a tracer instance.
   */
  public static Meter getMeter(String instrumentationName, String instrumentationVersion) {
    return get().get(instrumentationName, instrumentationVersion);
  }

  /**
   * Load provider class via {@link ServiceLoader}. A specific provider class can be requested via
   * setting a system property with FQCN.
   *
   * @return a provider or null if not found
   * @throws IllegalStateException if a specified provider is not found
   */
  @Nullable
  private static MeterProviderFactory loadSpi() {
    String specifiedProvider = System.getProperty(MeterProviderFactory.class.getName());
    ServiceLoader<MeterProviderFactory> providers = ServiceLoader.load(MeterProviderFactory.class);
    for (MeterProviderFactory provider : providers) {
      if (specifiedProvider == null || specifiedProvider.equals(provider.getClass().getName())) {
        return provider;
      }
    }
    if (specifiedProvider != null) {
      throw new IllegalStateException(
          String.format("Service provider %s not found", specifiedProvider));
    }
    return null;
  }
}
