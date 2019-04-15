/*
 * Copyright 2019, OpenConsensus Authors
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

package openconsensus.tags.propagation;

import openconsensus.tags.TagMap;

/**
 * Object containing all supported {@link TagMap} propagation formats.
 *
 * @since 0.1.0
 */
public abstract class PropagationComponent {

  /**
   * Returns the {@link BinaryFormat} for this implementation.
   *
   * @return the {@code BinaryFormat} for this implementation.
   * @since 0.1.0
   */
  public abstract BinaryFormat getBinaryFormat();

  /**
   * Returns the {@link TextFormat} for this implementation.
   *
   * <p>Usually this will be the W3C Correlation Context as the HTTP text format. For more details,
   * see <a href="https://github.com/w3c/correlation-context">correlation-context</a>.
   *
   * @return the {@code TextFormat} for this implementation.
   * @since 0.1.0
   */
  public abstract TextFormat getTextFormat();
}
