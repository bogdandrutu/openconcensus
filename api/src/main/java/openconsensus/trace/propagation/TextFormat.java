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

package openconsensus.trace.propagation;

import java.util.List;
import javax.annotation.Nullable;
import openconsensus.common.ExperimentalApi;
import openconsensus.trace.SpanContext;

/**
 * Injects and extracts {@link SpanContext trace identifiers} as text into carriers that travel
 * in-band across process boundaries. Identifiers are often encoded as messaging or RPC request
 * headers.
 *
 * <p>When using http, the carrier of propagated data on both the client (injector) and server
 * (extractor) side is usually an http request. Propagation is usually implemented via library-
 * specific request interceptors, where the client-side injects span identifiers and the server-side
 * extracts them.
 *
 * <p>Example of usage on the client:
 *
 * <pre>{@code
 * private static final Tracer tracer = Tracing.getTracer();
 * private static final TextFormat textFormat = Tracing.getPropagationComponent().getTextFormat();
 * private static final TextFormat.Setter setter = new TextFormat.Setter<HttpURLConnection>() {
 *   public void put(HttpURLConnection carrier, String key, String value) {
 *     carrier.setRequestProperty(field, value);
 *   }
 * }
 *
 * void makeHttpRequest() {
 *   Span span = tracer.spanBuilder("Sent.MyRequest").startSpan();
 *   try (Scope s = tracer.withSpan(span)) {
 *     HttpURLConnection connection =
 *         (HttpURLConnection) new URL("http://myserver").openConnection();
 *     textFormat.inject(span.getContext(), connection, httpURLConnectionSetter);
 *     // Send the request, wait for response and maybe set the status if not ok.
 *   }
 *   span.end();  // Can set a status.
 * }
 * }</pre>
 *
 * <p>Example of usage on the server:
 *
 * <pre>{@code
 * private static final Tracer tracer = Tracing.getTracer();
 * private static final TextFormat textFormat = Tracing.getPropagationComponent().getTextFormat();
 * private static final TextFormat.Getter<HttpRequest> getter = ...;
 *
 * void onRequestReceived(HttpRequest request) {
 *   SpanContext spanContext = textFormat.extract(request, getter);
 *   Span span = tracer.spanBuilderWithRemoteParent("Recv.MyRequest", spanContext).startSpan();
 *   try (Scope s = tracer.withSpan(span)) {
 *     // Handle request and send response back.
 *   }
 *   span.end()
 * }
 * }</pre>
 *
 * @since 0.1.0
 */
@ExperimentalApi
public abstract class TextFormat {
  /**
   * The propagation fields defined. If your carrier is reused, you should delete the fields here
   * before calling {@link #inject(SpanContext, Object, Setter)}.
   *
   * <p>For example, if the carrier is a single-use or immutable request object, you don't need to
   * clear fields as they couldn't have been set before. If it is a mutable, retryable object,
   * successive calls should clear these fields first.
   *
   * @return list of fields that will be used by this formatter.
   * @since 0.1.0
   */
  // The use cases of this are:
  // * allow pre-allocation of fields, especially in systems like gRPC Metadata
  // * allow a single-pass over an iterator (ex OpenTracing has no getter in TextMap)
  public abstract List<String> fields();

  /**
   * Injects the span context downstream. For example, as http headers.
   *
   * @param spanContext possibly not sampled.
   * @param carrier holds propagation fields. For example, an outgoing message or http request.
   * @param setter invoked for each propagation key to add or remove.
   * @param <C> carrier of propagation fields, such as an http request
   * @since 0.1.0
   */
  public abstract <C> void inject(SpanContext spanContext, C carrier, Setter<C> setter);

  /**
   * Class that allows a {@code TextFormat} to set propagated fields into a carrier.
   *
   * <p>{@code Setter} is stateless and allows to be saved as a constant to avoid runtime
   * allocations.
   *
   * @param <C> carrier of propagation fields, such as an http request
   * @since 0.1.0
   */
  public abstract static class Setter<C> {

    /**
     * Replaces a propagated field with the given value.
     *
     * <p>For example, a setter for an {@link java.net.HttpURLConnection} would be the method
     * reference {@link java.net.HttpURLConnection#addRequestProperty(String, String)}
     *
     * @param carrier holds propagation fields. For example, an outgoing message or http request.
     * @param key the key of the field.
     * @param value the value of the field.
     * @since 0.1.0
     */
    public abstract void put(C carrier, String key, String value);
  }

  /**
   * Extracts the span context from upstream. For example, as http headers.
   *
   * @param carrier holds propagation fields. For example, an outgoing message or http request.
   * @param getter invoked for each propagation key to get.
   * @param <C> carrier of propagation fields, such as an http request.
   * @return carrier of propagated fields or {@code SpanContext.INVALID} if it could not be parsed.
   * @since 0.1.0
   */
  public abstract <C> SpanContext extract(C carrier, Getter<C> getter);

  /**
   * Class that allows a {@code TextFormat} to read propagated fields from a carrier.
   *
   * <p>{@code Getter} is stateless and allows to be saved as a constant to avoid runtime
   * allocations.
   *
   * @param <C> carrier of propagation fields, such as an http request.
   * @since 0.1.0
   */
  public abstract static class Getter<C> {

    /**
     * Returns the first value of the given propagation {@code key} or returns {@code null}.
     *
     * @param carrier carrier of propagation fields, such as an http request.
     * @param key the key of the field.
     * @return the first value of the given propagation {@code key} or returns {@code null}.
     * @since 0.1.0
     */
    @Nullable
    public abstract String get(C carrier, String key);
  }
}
