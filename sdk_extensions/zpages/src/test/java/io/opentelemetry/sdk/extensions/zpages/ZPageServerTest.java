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

package io.opentelemetry.sdk.extensions.zpages;

import static com.google.common.truth.Truth.assertThat;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link ZPageServer}. */
@RunWith(JUnit4.class)
public final class ZPageServerTest {
  @Test
  public void tracezSpanProcessorOnlyAddedOnce() throws IOException {
    // tracezSpanProcessor is not added yet
    assertThat(ZPageServer.getIsTracezSpanProcesserAdded()).isFalse();
    HttpServer server = HttpServer.create(new InetSocketAddress(8888), 5);
    ZPageServer.registerAllPagesToHttpServer(server);
    // tracezSpanProcessor is added
    assertThat(ZPageServer.getIsTracezSpanProcesserAdded()).isTrue();
  }
}
