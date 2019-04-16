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
 * private static final Tracer tracer = Trace.getTracer();
 * private static final TextFormat textFormat = Trace.getTracer().getTextFormat();
 * private static final TextFormat.Setter setter = new TextFormat.Setter<HttpURLConnection>() {
 *   public void put(HttpURLConnection carrier, String key, String value) {
 *     carrier.setRequestProperty(field, value);
 *   }
 * }
 *
 * void makeHttpRequest() {
 *   Span span = tracer.spanBuilder("MyRequest").setSpanKind(Span.Kind.CLIENT).startSpan();
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
 * private static final Tracer tracer = Trace.getTracer();
 * private static final TextFormat textFormat = Trace.getTracer().getTextFormat();
 * private static final TextFormat.Getter<HttpRequest> getter = ...;
 *
 * void onRequestReceived(HttpRequest request) {
 *   SpanContext spanContext = textFormat.extract(request, getter);
 *   Span span = tracer.spanBuilderWithRemoteParent("MyRequest", spanContext)
 *       .setSpanKind(Span.Kind.SERVER).startSpan();
 *   try (Scope s = tracer.withSpan(span)) {
 *     // Handle request and send response back.
 *   }
 *   span.end()
 * }
 * }</pre>
 *
 * @since 0.1.0
 */
public abstract class TextFormat
    extends openconsensus.context.propagation.TextFormat<SpanContext> {}
