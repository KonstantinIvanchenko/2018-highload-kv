/*
 * Copyright 2018 (c) Vadim Tsesko <incubos@yandex.com>
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

package ru.mail.polis;

import com.google.common.collect.Iterators;
import one.nio.http.HttpClient;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Facilities for cluster tests
 *
 * @author Vadim Tsesko <incubos@yandex.com>
 */
abstract class ClusterTestBase extends TestBase {
    private static final long TIMEOUT = Duration.ofSeconds(60).toMillis();//TimeUnit.SECONDS.toMillis(1);
    private final Map<String, HttpClient> hostToClient = new HashMap<>();
    Set<String> endpoints;

    @NotNull
    private HttpClient client(final int node) {
        final String endpoint = Iterators.get(endpoints.iterator(), node);
        return hostToClient.computeIfAbsent(
                endpoint,
                key -> new HttpClient(new ConnectionString(key + "?timeout=" + TIMEOUT)));
    }

    private void resetClient(final int node) {
        final String endpoint = Iterators.get(endpoints.iterator(), node);
        final HttpClient client = hostToClient.remove(endpoint);
        if (client != null) {
            client.close();
        }
    }

    void stop(
            final int node,
            @NotNull final KVService service) {
        resetClient(node);
        service.stop();
    }

    void start(
            final int node,
            @NotNull final KVService service) {
        service.start();

        // Wait for the server to answer
        final long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(1);
        while (System.currentTimeMillis() < deadline) {
            try {
                if (client(node).get("/v0/status").getStatus() == 200) {
                    return;
                }
            } catch (Exception ignored) {
            }
        }
        throw new RuntimeException("Can't wait for the service");
    }

    @NotNull
    private String path(
            @NotNull final String id,
            final int ack,
            final int from) {
        return "/v0/entity?id=" + id + "&replicas=" + ack + "/" + from;
    }

    Response get(
            final int node,
            @NotNull final String key,
            final int ack,
            final int from) throws Exception {
        return client(node).get(path(key, ack, from));
    }

    Response delete(
            final int node,
            @NotNull final String key,
            final int ack,
            final int from) throws Exception {
        return client(node).delete(path(key, ack, from));
    }

    Response upsert(
            final int node,
            @NotNull final String key,
            @NotNull final byte[] data,
            final int ack,
            final int from) throws Exception {
        return client(node).put(path(key, ack, from), data);
    }
}
