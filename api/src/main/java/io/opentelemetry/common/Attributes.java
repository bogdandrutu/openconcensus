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

package io.opentelemetry.common;

import static io.opentelemetry.common.AttributesKeys.booleanArrayKey;
import static io.opentelemetry.common.AttributesKeys.booleanKey;
import static io.opentelemetry.common.AttributesKeys.doubleArrayKey;
import static io.opentelemetry.common.AttributesKeys.doubleKey;
import static io.opentelemetry.common.AttributesKeys.longArrayKey;
import static io.opentelemetry.common.AttributesKeys.longKey;
import static io.opentelemetry.common.AttributesKeys.stringArrayKey;
import static io.opentelemetry.common.AttributesKeys.stringKey;

import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import javax.annotation.concurrent.Immutable;

/**
 * An immutable container for attributes.
 *
 * <p>The keys are {@link AttributeKey}s and the values are Object instances that match the type of
 * the provided key.
 */
@SuppressWarnings("rawtypes")
@Immutable
public abstract class Attributes extends ImmutableKeyValuePairs<AttributeKey, Object>
    implements ReadableAttributes {
  private static final Attributes EMPTY = Attributes.newBuilder().build();

  @AutoValue
  @Immutable
  abstract static class ArrayBackedAttributes extends Attributes {
    ArrayBackedAttributes() {}

    @Override
    abstract List<Object> data();

    @Override
    public Builder toBuilder() {
      return new Builder(new ArrayList<>(data()));
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T get(AttributeKey<T> key) {
    return (T) super.get(key);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void forEach(AttributeConsumer consumer) {
    List<Object> data = data();
    for (int i = 0; i < data.size(); i += 2) {
      consumer.consume((AttributeKey) data.get(i), data.get(i + 1));
    }
  }

  /** Returns a {@link Attributes} instance with no attributes. */
  public static Attributes empty() {
    return EMPTY;
  }

  /** Returns a {@link Attributes} instance with a single key-value pair. */
  public static <T> Attributes of(AttributeKey<T> key, T value) {
    return sortAndFilterToAttributes(key, value);
  }

  /**
   * Returns a {@link Attributes} instance with two key-value pairs. Order of the keys is not
   * preserved. Duplicate keys will be removed.
   */
  public static <T, U> Attributes of(
      AttributeKey<T> key1, T value1, AttributeKey<U> key2, U value2) {
    return sortAndFilterToAttributes(key1, value1, key2, value2);
  }

  /**
   * Returns a {@link Attributes} instance with three key-value pairs. Order of the keys is not
   * preserved. Duplicate keys will be removed.
   */
  public static <T, U, V> Attributes of(
      AttributeKey<T> key1,
      T value1,
      AttributeKey<U> key2,
      U value2,
      AttributeKey<V> key3,
      V value3) {
    return sortAndFilterToAttributes(key1, value1, key2, value2, key3, value3);
  }

  /**
   * Returns a {@link Attributes} instance with four key-value pairs. Order of the keys is not
   * preserved. Duplicate keys will be removed.
   */
  public static <T, U, V, W> Attributes of(
      AttributeKey<T> key1,
      T value1,
      AttributeKey<U> key2,
      U value2,
      AttributeKey<V> key3,
      V value3,
      AttributeKey<W> key4,
      W value4) {
    return sortAndFilterToAttributes(key1, value1, key2, value2, key3, value3, key4, value4);
  }

  /**
   * Returns a {@link Attributes} instance with five key-value pairs. Order of the keys is not
   * preserved. Duplicate keys will be removed.
   */
  public static <T, U, V, W, X> Attributes of(
      AttributeKey<T> key1,
      T value1,
      AttributeKey<U> key2,
      U value2,
      AttributeKey<V> key3,
      V value3,
      AttributeKey<W> key4,
      W value4,
      AttributeKey<X> key5,
      X value5) {
    return sortAndFilterToAttributes(
        key1, value1,
        key2, value2,
        key3, value3,
        key4, value4,
        key5, value5);
  }

  private static Attributes sortAndFilterToAttributes(Object... data) {
    // null out any empty keys
    for (int i = 0; i < data.length; i += 2) {
      AttributeKey<?> key = (AttributeKey<?>) data[i];
      if (key != null && (key.getKey() == null || "".equals(key.getKey()))) {
        data[i] = null;
      }
    }
    return new AutoValue_Attributes_ArrayBackedAttributes(sortAndFilter(data));
  }

  /** Returns a new {@link Builder} instance for creating arbitrary {@link Attributes}. */
  public static Builder newBuilder() {
    return new Builder();
  }

  /** Returns a new {@link Builder} instance from ReadableAttributes. */
  public static Builder newBuilder(ReadableAttributes attributes) {
    final Builder builder = new Builder();
    attributes.forEach(
        new AttributeConsumer() {
          @Override
          public <T> void consume(AttributeKey<T> key, T value) {
            builder.setAttribute(key, value);
          }
        });
    return builder;
  }

  /** Returns a new {@link Builder} instance populated with the data of this {@link Attributes}. */
  public abstract Builder toBuilder();

  /**
   * Enables the creation of an {@link Attributes} instance with an arbitrary number of key-value
   * pairs.
   */
  public static class Builder {
    private final List<Object> data;

    private Builder() {
      data = new ArrayList<>();
    }

    private Builder(List<Object> data) {
      this.data = data;
    }

    /** Create the {@link Attributes} from this. */
    public Attributes build() {
      return sortAndFilterToAttributes(data.toArray());
    }

    /** Sets a {@link AttributeKey} with associated value into this. */
    public <T> Builder setAttribute(AttributeKey<T> key, T value) {
      if (key == null || key.getKey() == null || key.getKey().length() == 0) {
        return this;
      }
      if (value == null) {
        // Remove key/value pairs
        Iterator<Object> itr = data.iterator();
        while (itr.hasNext()) {
          AttributeKey k = (AttributeKey) itr.next();
          if (key.equals(k)) {
            // delete key and value
            itr.remove();
            itr.next();
            itr.remove();
          } else {
            // skip the value part
            itr.next();
          }
        }
        return this;
      }
      data.add(key);
      data.add(value);
      return this;
    }

    /**
     * Sets a String attribute into this.
     *
     * <p>Note: It is strongly recommended to use {@link #setAttribute(AttributeKey, Object)}, and
     * pre-allocate your keys, if possible.
     *
     * @return this Builder
     */
    public Builder setAttribute(String key, String value) {
      return setAttribute(stringKey(key), value);
    }

    /**
     * Sets a long attribute into this.
     *
     * <p>Note: It is strongly recommended to use {@link #setAttribute(AttributeKey, Object)}, and
     * pre-allocate your keys, if possible.
     *
     * @return this Builder
     */
    public Builder setAttribute(String key, long value) {
      return setAttribute(longKey(key), value);
    }

    /**
     * Sets a double attribute into this.
     *
     * <p>Note: It is strongly recommended to use {@link #setAttribute(AttributeKey, Object)}, and
     * pre-allocate your keys, if possible.
     *
     * @return this Builder
     */
    public Builder setAttribute(String key, double value) {
      return setAttribute(doubleKey(key), value);
    }

    /**
     * Sets a boolean attribute into this.
     *
     * <p>Note: It is strongly recommended to use {@link #setAttribute(AttributeKey, Object)}, and
     * pre-allocate your keys, if possible.
     *
     * @return this Builder
     */
    public Builder setAttribute(String key, boolean value) {
      return setAttribute(booleanKey(key), value);
    }

    /**
     * Sets a String array attribute into this.
     *
     * <p>Note: It is strongly recommended to use {@link #setAttribute(AttributeKey, Object)}, and
     * pre-allocate your keys, if possible.
     *
     * @return this Builder
     */
    public Builder setAttribute(String key, String... value) {
      return setAttribute(stringArrayKey(key), value == null ? null : Arrays.asList(value));
    }

    /**
     * Sets a Long array attribute into this.
     *
     * <p>Note: It is strongly recommended to use {@link #setAttribute(AttributeKey, Object)}, and
     * pre-allocate your keys, if possible.
     *
     * @return this Builder
     */
    public Builder setAttribute(String key, Long... value) {
      return setAttribute(longArrayKey(key), value == null ? null : Arrays.asList(value));
    }

    /**
     * Sets a Double array attribute into this.
     *
     * <p>Note: It is strongly recommended to use {@link #setAttribute(AttributeKey, Object)}, and
     * pre-allocate your keys, if possible.
     *
     * @return this Builder
     */
    public Builder setAttribute(String key, Double... value) {
      return setAttribute(doubleArrayKey(key), value == null ? null : Arrays.asList(value));
    }

    /**
     * Sets a Boolean array attribute into this.
     *
     * <p>Note: It is strongly recommended to use {@link #setAttribute(AttributeKey, Object)}, and
     * pre-allocate your keys, if possible.
     *
     * @return this Builder
     */
    public Builder setAttribute(String key, Boolean... value) {
      return setAttribute(booleanArrayKey(key), value == null ? null : Arrays.asList(value));
    }

    /**
     * Add all the provided attributes to this Builder.
     *
     * @return this Builder
     */
    public Builder addAll(Attributes attributes) {
      data.addAll(attributes.data());
      return this;
    }
  }
}
