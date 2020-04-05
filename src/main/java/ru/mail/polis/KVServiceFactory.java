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

import org.jetbrains.annotations.NotNull;
import service.HttpService;

import java.io.IOException;
import java.util.Set;

/**
 * Constructs {@link KVService} instances.
 *
 * @author Vadim Tsesko <mail@incubos.org>
 */
final class KVServiceFactory {
    private static final long MAX_HEAP = 128 * 1024 * 1024;

    private KVServiceFactory() {
        // Not supposed to be instantiated
    }

    /**
     * Construct a storage instance.
     *
     * @param port     port to bind HTTP server to
     * @param dao      DAO to store the data
     * @param topology a list of all cluster endpoints {@code http://<host>:<port>} (including this one)
     * @return a storage instance
     */
    @NotNull
    static KVService create(
            final int port,
            @NotNull final KVDao dao,
            @NotNull final Set<String> topology) throws IOException {
        if (Runtime.getRuntime().maxMemory() > MAX_HEAP) {
            throw new IllegalStateException("The heap is too big. Consider setting Xmx.");
        }

        if (port <= 0 || 65536 <= port) {
            throw new IllegalArgumentException("Port out of range");
        }

        return new HttpService(port, dao, topology);
    }
}
