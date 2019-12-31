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

package io.opentelemetry;

import io.opentelemetry.distributedcontext.DefaultDistributedContextManager;
import io.opentelemetry.distributedcontext.DistributedContextManager;
import io.opentelemetry.distributedcontext.spi.DistributedContextManagerProvider;
import io.opentelemetry.metrics.DefaultMeterRegistry;
import io.opentelemetry.metrics.DefaultMeterRegistryProvider;
import io.opentelemetry.metrics.Meter;
import io.opentelemetry.metrics.MeterRegistry;
import io.opentelemetry.metrics.spi.MeterRegistryProvider;
import io.opentelemetry.trace.DefaultTracerRegistry;
import io.opentelemetry.trace.DefaultTracerRegistryProvider;
import io.opentelemetry.trace.Tracer;
import io.opentelemetry.trace.TracerRegistry;
import io.opentelemetry.trace.spi.TracerRegistryProvider;
import java.util.ServiceLoader;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * This class provides a static global accessor for telemetry objects {@link Tracer}, {@link Meter}
 * and {@link DistributedContextManager}.
 *
 * <p>The telemetry objects are lazy-loaded singletons resolved via {@link ServiceLoader} mechanism.
 *
 * @see TracerRegistry
 * @see MeterRegistryProvider
 * @see DistributedContextManagerProvider
 */
@ThreadSafe
public final class OpenTelemetry {

  @Nullable private static volatile OpenTelemetry instance;

  private final TracerRegistry tracerRegistry;
  private final MeterRegistry meterRegistry;
  private final DistributedContextManager contextManager;

  /**
   * Returns a singleton {@link TracerRegistry}.
   *
   * @return registered TracerRegistry of default via {@link DefaultTracerRegistry#getInstance()}.
   * @throws IllegalStateException if a specified TracerRegistry (via system properties) could not
   *     be found.
   * @since 0.1.0
   */
  public static TracerRegistry getTracerRegistry() {
    return getInstance().tracerRegistry;
  }

  /**
   * Returns a singleton {@link MeterRegistry}.
   *
   * @return registered MeterRegistry or default via {@link DefaultMeterRegistry#getInstance()}.
   * @throws IllegalStateException if a specified MeterRegistry (via system properties) could not be
   *     found.
   * @since 0.1.0
   */
  public static MeterRegistry getMeterRegistry() {
    return getInstance().meterRegistry;
  }

  /**
   * Returns a singleton {@link DistributedContextManager}.
   *
   * @return registered manager or default via {@link
   *     DefaultDistributedContextManager#getInstance()}.
   * @throws IllegalStateException if a specified manager (via system properties) could not be
   *     found.
   * @since 0.1.0
   */
  public static DistributedContextManager getDistributedContextManager() {
    return getInstance().contextManager;
  }

  /** Lazy loads an instance. */
  private static OpenTelemetry getInstance() {
    if (instance == null) {
      synchronized (OpenTelemetry.class) {
        if (instance == null) {
          instance = new OpenTelemetry();
        }
      }
    }
    return instance;
  }

  private OpenTelemetry() {
    TracerRegistryProvider tracerRegistryProvider = loadSpi(TracerRegistryProvider.class);
    this.tracerRegistry =
        tracerRegistryProvider != null
            ? tracerRegistryProvider.create()
            : DefaultTracerRegistryProvider.getInstance().create();

    MeterRegistryProvider meterRegistryProvider = loadSpi(MeterRegistryProvider.class);
    meterRegistry =
        meterRegistryProvider != null
            ? meterRegistryProvider.create()
            : DefaultMeterRegistryProvider.getInstance().create();
    DistributedContextManagerProvider contextManagerProvider =
        loadSpi(DistributedContextManagerProvider.class);
    contextManager =
        contextManagerProvider != null
            ? contextManagerProvider.create()
            : DefaultDistributedContextManager.getInstance();
  }

  /**
   * Load provider class via {@link ServiceLoader}. A specific provider class can be requested via
   * setting a system property with FQCN.
   *
   * @param providerClass a provider class
   * @param <T> provider type
   * @return a provider or null if not found
   * @throws IllegalStateException if a specified provider is not found
   */
  @Nullable
  private static <T> T loadSpi(Class<T> providerClass) {
    String specifiedProvider = System.getProperty(providerClass.getName());
    ServiceLoader<T> providers = ServiceLoader.load(providerClass);
    for (T provider : providers) {
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

  // for testing
  static void reset() {
    instance = null;
  }
}
