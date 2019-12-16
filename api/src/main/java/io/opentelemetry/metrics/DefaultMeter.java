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

package io.opentelemetry.metrics;

import io.opentelemetry.internal.StringUtils;
import io.opentelemetry.internal.Utils;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.Immutable;

/**
 * No-op implementations of {@link Meter}.
 *
 * @since 0.1.0
 */
public final class DefaultMeter implements Meter {
  private static final DefaultMeter INSTANCE = new DefaultMeter();

  /* VisibleForTesting */ static final int NAME_MAX_LENGTH = 255;
  /* VisibleForTesting */ static final String ERROR_MESSAGE_INVALID_NAME =
      "Name should be a ASCII string with a length no greater than "
          + NAME_MAX_LENGTH
          + " characters.";

  /**
   * Returns a {@code Meter} singleton that is the default implementations for {@link Meter}.
   *
   * @return a {@code Meter} singleton that is the default implementations for {@link Meter}.
   * @since 0.1.0
   */
  public static Meter getInstance() {
    return INSTANCE;
  }

  @Override
  public GaugeLong.Builder gaugeLongBuilder(String name) {
    Utils.checkNotNull(name, "name");
    Utils.checkArgument(
        StringUtils.isPrintableString(name) && name.length() <= NAME_MAX_LENGTH,
        ERROR_MESSAGE_INVALID_NAME);
    return new NoopGaugeLong.NoopBuilder();
  }

  @Override
  public GaugeDouble.Builder gaugeDoubleBuilder(String name) {
    Utils.checkNotNull(name, "name");
    Utils.checkArgument(
        StringUtils.isPrintableString(name) && name.length() <= NAME_MAX_LENGTH,
        ERROR_MESSAGE_INVALID_NAME);
    return new NoopGaugeDouble.NoopBuilder();
  }

  @Override
  public CounterDouble.Builder counterDoubleBuilder(String name) {
    Utils.checkNotNull(name, "name");
    Utils.checkArgument(
        StringUtils.isPrintableString(name) && name.length() <= NAME_MAX_LENGTH,
        ERROR_MESSAGE_INVALID_NAME);
    return new NoopCounterDouble.NoopBuilder();
  }

  @Override
  public CounterLong.Builder counterLongBuilder(String name) {
    Utils.checkNotNull(name, "name");
    Utils.checkArgument(
        StringUtils.isPrintableString(name) && name.length() <= NAME_MAX_LENGTH,
        ERROR_MESSAGE_INVALID_NAME);
    return new NoopCounterLong.NoopBuilder();
  }

  @Override
  public MeasureDouble.Builder measureDoubleBuilder(String name) {
    Utils.checkNotNull(name, "name");
    Utils.checkArgument(
        StringUtils.isPrintableString(name) && name.length() <= NAME_MAX_LENGTH,
        ERROR_MESSAGE_INVALID_NAME);
    return new NoopMeasureDouble.NoopBuilder();
  }

  @Override
  public MeasureLong.Builder measureLongBuilder(String name) {
    Utils.checkNotNull(name, "name");
    Utils.checkArgument(
        StringUtils.isPrintableString(name) && name.length() <= NAME_MAX_LENGTH,
        ERROR_MESSAGE_INVALID_NAME);
    return new NoopMeasureLong.NoopBuilder();
  }

  @Override
  public ObserverDouble.Builder observerDoubleBuilder(String name) {
    Utils.checkNotNull(name, "name");
    Utils.checkArgument(
        StringUtils.isPrintableString(name) && name.length() <= NAME_MAX_LENGTH,
        ERROR_MESSAGE_INVALID_NAME);
    return new NoopObserverDouble.NoopBuilder();
  }

  @Override
  public ObserverLong.Builder observerLongBuilder(String name) {
    Utils.checkNotNull(name, "name");
    Utils.checkArgument(
        StringUtils.isPrintableString(name) && name.length() <= NAME_MAX_LENGTH,
        ERROR_MESSAGE_INVALID_NAME);
    return new NoopObserverLong.NoopBuilder();
  }

  @Override
  public BatchRecorder newMeasureBatchRecorder() {
    return new NoopBatchRecorder();
  }

  @Override
  public LabelSet createLabelSet(String k1, String v1) {
    Utils.checkNotNull(k1, "k1");
    Utils.checkNotNull(v1, "v1");
    return NoopLabelSet.INSTANCE;
  }

  @Override
  public LabelSet createLabelSet(String k1, String v1, String k2, String v2) {
    Utils.checkNotNull(k1, "k1");
    Utils.checkNotNull(v1, "v1");
    Utils.checkNotNull(k2, "k2");
    Utils.checkNotNull(v2, "v2");
    return NoopLabelSet.INSTANCE;
  }

  @Override
  public LabelSet createLabelSet(String k1, String v1, String k2, String v2, String k3, String v3) {
    Utils.checkNotNull(k1, "k1");
    Utils.checkNotNull(v1, "v1");
    Utils.checkNotNull(k2, "k2");
    Utils.checkNotNull(v2, "v2");
    Utils.checkNotNull(k3, "k3");
    Utils.checkNotNull(v3, "v3");
    return NoopLabelSet.INSTANCE;
  }

  @Override
  public LabelSet createLabelSet(
      String k1, String v1, String k2, String v2, String k3, String v3, String k4, String v4) {
    Utils.checkNotNull(k1, "k1");
    Utils.checkNotNull(v1, "v1");
    Utils.checkNotNull(k2, "k2");
    Utils.checkNotNull(v2, "v2");
    Utils.checkNotNull(k3, "k3");
    Utils.checkNotNull(v3, "v3");
    Utils.checkNotNull(k4, "k4");
    Utils.checkNotNull(v4, "v4");
    return NoopLabelSet.INSTANCE;
  }

  /** No-op implementations of GaugeLong class. */
  @Immutable
  private static final class NoopGaugeLong implements GaugeLong {
    /** Creates a new {@code NoopBound}. */
    private NoopGaugeLong() {}

    @Override
    public NoopBoundLongGauge bind(LabelSet labelSet) {
      Utils.checkNotNull(labelSet, "labelSet");
      return NoopBoundLongGauge.INSTANCE;
    }

    @Override
    public NoopBoundLongGauge getDefaultBound() {
      return NoopBoundLongGauge.INSTANCE;
    }

    @Override
    public void unbind(BoundLongGauge boundLongGauge) {
      Utils.checkNotNull(boundLongGauge, "boundLongGauge");
    }

    /** No-op implementations of Bound class. */
    @Immutable
    private enum NoopBoundLongGauge implements BoundLongGauge {
      INSTANCE;

      @Override
      public void set(long val) {}
    }

    private static final class NoopBuilder extends NoopAbstractGaugeBuilder<Builder, GaugeLong>
        implements Builder {
      @Override
      protected Builder getThis() {
        return this;
      }

      @Override
      public GaugeLong build() {
        return new NoopGaugeLong();
      }
    }
  }

  /** No-op implementations of GaugeDouble class. */
  @Immutable
  private static final class NoopGaugeDouble implements GaugeDouble {
    /** Creates a new {@code NoopBound}. */
    private NoopGaugeDouble() {}

    @Override
    public NoopBoundDoubleGauge bind(LabelSet labelSet) {
      Utils.checkNotNull(labelSet, "labelSet");
      return NoopBoundDoubleGauge.INSTANCE;
    }

    @Override
    public NoopBoundDoubleGauge getDefaultBound() {
      return NoopBoundDoubleGauge.INSTANCE;
    }

    @Override
    public void unbind(BoundDoubleGauge boundDoubleGauge) {
      Utils.checkNotNull(boundDoubleGauge, "boundDoubleGauge");
    }

    /** No-op implementations of Bound class. */
    @Immutable
    private enum NoopBoundDoubleGauge implements BoundDoubleGauge {
      INSTANCE;

      @Override
      public void set(double val) {}
    }

    private static final class NoopBuilder extends NoopAbstractGaugeBuilder<Builder, GaugeDouble>
        implements Builder {
      @Override
      protected Builder getThis() {
        return this;
      }

      @Override
      public GaugeDouble build() {
        return new NoopGaugeDouble();
      }
    }
  }

  /** No-op implementations of CounterDouble class. */
  @Immutable
  private static final class NoopCounterDouble implements CounterDouble {
    /** Creates a new {@code NoopBound}. */
    private NoopCounterDouble() {}

    @Override
    public NoopBoundDoubleCounter bind(LabelSet labelSet) {
      Utils.checkNotNull(labelSet, "labelSet");
      return NoopBoundDoubleCounter.INSTANCE;
    }

    @Override
    public NoopBoundDoubleCounter getDefaultBound() {
      return NoopBoundDoubleCounter.INSTANCE;
    }

    @Override
    public void unbind(BoundDoubleCounter boundDoubleCounter) {
      Utils.checkNotNull(boundDoubleCounter, "boundDoubleCounter");
    }

    /** No-op implementations of Bound class. */
    @Immutable
    private enum NoopBoundDoubleCounter implements BoundDoubleCounter {
      INSTANCE;

      @Override
      public void add(double delta) {}
    }

    private static final class NoopBuilder
        extends NoopAbstractCounterBuilder<Builder, CounterDouble> implements Builder {
      @Override
      protected Builder getThis() {
        return this;
      }

      @Override
      public CounterDouble build() {
        return new NoopCounterDouble();
      }
    }
  }

  /** No-op implementations of CounterLong class. */
  @Immutable
  private static final class NoopCounterLong implements CounterLong {
    /** Creates a new {@code NoopBound}. */
    private NoopCounterLong() {}

    @Override
    public NoopBoundLongCounter bind(LabelSet labelSet) {
      Utils.checkNotNull(labelSet, "labelSet");
      return NoopBoundLongCounter.INSTANCE;
    }

    @Override
    public NoopBoundLongCounter getDefaultBound() {
      return NoopBoundLongCounter.INSTANCE;
    }

    @Override
    public void unbind(BoundLongCounter boundLongCounter) {
      Utils.checkNotNull(boundLongCounter, "boundLongCounter");
    }

    /** No-op implementations of Bound class. */
    @Immutable
    private enum NoopBoundLongCounter implements BoundLongCounter {
      INSTANCE;

      @Override
      public void add(long delta) {}
    }

    private static final class NoopBuilder extends NoopAbstractCounterBuilder<Builder, CounterLong>
        implements Builder {
      @Override
      protected Builder getThis() {
        return this;
      }

      @Override
      public CounterLong build() {
        return new NoopCounterLong();
      }
    }
  }

  @Immutable
  private static final class NoopMeasureDouble implements MeasureDouble {
    /** Creates a new {@code NoopBound}. */
    private NoopMeasureDouble() {}

    @Override
    public NoopBoundDoubleMeasure bind(LabelSet labelSet) {
      Utils.checkNotNull(labelSet, "labelSet");
      return NoopBoundDoubleMeasure.INSTANCE;
    }

    @Override
    public NoopBoundDoubleMeasure getDefaultBound() {
      return NoopBoundDoubleMeasure.INSTANCE;
    }

    @Override
    public void unbind(BoundDoubleMeasure boundDoubleMeasure) {
      Utils.checkNotNull(boundDoubleMeasure, "boundDoubleMeasure");
    }

    /** No-op implementations of Bound class. */
    @Immutable
    private enum NoopBoundDoubleMeasure implements BoundDoubleMeasure {
      INSTANCE;

      @Override
      public void record(double value) {
        Utils.checkArgument(value >= 0.0, "Unsupported negative values.");
      }
    }

    private static final class NoopBuilder extends NoopAbstractMetricBuilder<Builder, MeasureDouble>
        implements Builder {
      @Override
      protected Builder getThis() {
        return this;
      }

      @Override
      public MeasureDouble build() {
        return new NoopMeasureDouble();
      }
    }
  }

  @Immutable
  private static final class NoopMeasureLong implements MeasureLong {
    private NoopMeasureLong() {}

    @Override
    public NoopBoundLongMeasure bind(LabelSet labelSet) {
      Utils.checkNotNull(labelSet, "labelSet");
      return NoopBoundLongMeasure.INSTANCE;
    }

    @Override
    public NoopBoundLongMeasure getDefaultBound() {
      return NoopBoundLongMeasure.INSTANCE;
    }

    @Override
    public void unbind(BoundLongMeasure boundLongMeasure) {
      Utils.checkNotNull(boundLongMeasure, "boundLongMeasure");
    }

    /** No-op implementations of Bound class. */
    @Immutable
    private enum NoopBoundLongMeasure implements BoundLongMeasure {
      INSTANCE;

      @Override
      public void record(long value) {
        Utils.checkArgument(value >= 0, "Unsupported negative values.");
      }
    }

    private static final class NoopBuilder extends NoopAbstractMetricBuilder<Builder, MeasureLong>
        implements Builder {
      @Override
      protected Builder getThis() {
        return this;
      }

      @Override
      public MeasureLong build() {
        return new NoopMeasureLong();
      }
    }
  }

  @Immutable
  private static final class NoopObserverDouble implements ObserverDouble {
    private NoopObserverDouble() {}

    @Override
    public NoopBoundDoubleObserver bind(LabelSet labelSet) {
      Utils.checkNotNull(labelSet, "labelSet");
      return NoopBoundDoubleObserver.INSTANCE;
    }

    @Override
    public NoopBoundDoubleObserver getDefaultBound() {
      return NoopBoundDoubleObserver.INSTANCE;
    }

    @Override
    public void unbind(BoundDoubleObserver boundDoubleObserver) {
      Utils.checkNotNull(boundDoubleObserver, "boundDoubleObserver");
    }

    @Override
    public void setCallback(Callback<ResultDoubleObserver> metricUpdater) {
      Utils.checkNotNull(metricUpdater, "metricUpdater");
    }

    /** No-op implementations of Bound class. */
    @Immutable
    private enum NoopBoundDoubleObserver implements BoundDoubleObserver {
      INSTANCE
    }

    private static final class NoopBuilder
        extends NoopAbstractObserverBuilder<Builder, ObserverDouble> implements Builder {
      @Override
      protected Builder getThis() {
        return this;
      }

      @Override
      public ObserverDouble build() {
        return new NoopObserverDouble();
      }
    }
  }

  @Immutable
  private static final class NoopObserverLong implements ObserverLong {
    private NoopObserverLong() {}

    @Override
    public NoopBoundLongObserver bind(LabelSet labelSet) {
      Utils.checkNotNull(labelSet, "labelSet");
      return NoopBoundLongObserver.INSTANCE;
    }

    @Override
    public NoopBoundLongObserver getDefaultBound() {
      return NoopBoundLongObserver.INSTANCE;
    }

    @Override
    public void unbind(BoundLongObserver boundLongObserver) {
      Utils.checkNotNull(boundLongObserver, "boundLongObserver");
    }

    @Override
    public void setCallback(Callback<ResultLongObserver> metricUpdater) {
      Utils.checkNotNull(metricUpdater, "metricUpdater");
    }

    /** No-op implementations of Bound class. */
    @Immutable
    private enum NoopBoundLongObserver implements BoundLongObserver {
      INSTANCE
    }

    private static final class NoopBuilder
        extends NoopAbstractObserverBuilder<Builder, ObserverLong> implements Builder {
      @Override
      protected Builder getThis() {
        return this;
      }

      @Override
      public ObserverLong build() {
        return new NoopObserverLong();
      }
    }
  }

  private static final class NoopBatchRecorder implements BatchRecorder {
    private NoopBatchRecorder() {}

    @Override
    public BatchRecorder put(MeasureLong measure, long value) {
      Utils.checkNotNull(measure, "measure");
      Utils.checkArgument(value >= 0, "Unsupported negative values.");
      return this;
    }

    @Override
    public BatchRecorder put(MeasureDouble measure, double value) {
      Utils.checkNotNull(measure, "measure");
      Utils.checkArgument(value >= 0.0, "Unsupported negative values.");
      return this;
    }

    @Override
    public void record() {}
  }

  private abstract static class NoopAbstractGaugeBuilder<B extends Gauge.Builder<B, V>, V>
      extends NoopAbstractMetricBuilder<B, V> implements Gauge.Builder<B, V> {
    @Override
    public B setMonotonic(boolean monotonic) {
      return getThis();
    }
  }

  private abstract static class NoopAbstractCounterBuilder<B extends Counter.Builder<B, V>, V>
      extends NoopAbstractMetricBuilder<B, V> implements Counter.Builder<B, V> {
    @Override
    public B setMonotonic(boolean monotonic) {
      return getThis();
    }
  }

  private abstract static class NoopAbstractObserverBuilder<B extends Observer.Builder<B, V>, V>
      extends NoopAbstractMetricBuilder<B, V> implements Observer.Builder<B, V> {
    @Override
    public B setMonotonic(boolean monotonic) {
      return getThis();
    }
  }

  private abstract static class NoopAbstractMetricBuilder<B extends Metric.Builder<B, V>, V>
      implements Metric.Builder<B, V> {
    @Override
    public B setDescription(String description) {
      Utils.checkNotNull(description, "description");
      return getThis();
    }

    @Override
    public B setUnit(String unit) {
      Utils.checkNotNull(unit, "unit");
      return getThis();
    }

    @Override
    public B setLabelKeys(List<String> labelKeys) {
      Utils.checkListElementNotNull(Utils.checkNotNull(labelKeys, "labelKeys"), "labelKey");
      return getThis();
    }

    @Override
    public B setConstantLabels(Map<String, String> constantLabels) {
      Utils.checkMapKeysNotNull(
          Utils.checkNotNull(constantLabels, "constantLabels"), "constantLabel");
      return getThis();
    }

    protected abstract B getThis();
  }

  private enum NoopLabelSet implements LabelSet {
    INSTANCE
  }
}
