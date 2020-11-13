/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.metrics.view;

import com.google.auto.value.AutoValue;
import com.google.auto.value.extension.memoized.Memoized;
import io.opentelemetry.sdk.metrics.common.InstrumentType;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/**
 * Provides means for selecting one ore more {@link io.opentelemetry.api.metrics.Instrument}s. Used
 * for configuring a aggregations for the specified instruments.
 *
 * <p>There are two options for selecting instruments: by instrument name and by instrument type.
 */
@AutoValue
@Immutable
public abstract class InstrumentSelector {
  public static Builder newBuilder() {
    return new AutoValue_InstrumentSelector.Builder();
  }

  /**
   * Returns {@link InstrumentType} that should be selected. If null, then this specifier will not be used.
   */
  @Nullable
  public abstract InstrumentType instrumentType();

  /**
   * Returns which instrument names should be selected. This is a regex. If null, then this specifier will
   * not be used.
   */
  @Nullable
  public abstract String instrumentNameRegex();

  /**
   * Returns the {@link Pattern} generated by the provided {@link #instrumentNameRegex()}, or null if none
   * was specified.
   */
  @Nullable
  @Memoized
  public Pattern instrumentNamePattern() {
    return instrumentNameRegex() == null ? null : Pattern.compile(instrumentNameRegex());
  }

  /** Returns whether the InstrumentType been specified. */
  public boolean hasInstrumentType() {
    return instrumentType() != null;
  }

  /** Returns whether the instrument name regex been specified. */
  public boolean hasInstrumentNameRegex() {
    return instrumentNameRegex() != null;
  }

  /** Builder for {@link InstrumentSelector} instances. */
  @AutoValue.Builder
  public interface Builder {
    /** Sets a specifier for {@link InstrumentType}. */
    Builder instrumentType(InstrumentType instrumentType);

    /** Sets a specifier for selecting Instruments by name. */
    Builder instrumentNameRegex(String regex);

    /** Returns an InstrumentSelector instance with the content of this builder. */
    InstrumentSelector build();
  }
}
