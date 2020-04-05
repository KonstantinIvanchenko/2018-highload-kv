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


import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Persistence tests for {@link KVDao} implementations
 *
 * @author Vadim Tsesko <incubos@yandex.com>
 */
class PersistenceTest extends TestBase {
    @Test()
    void fs() throws IOException {
        // Reference key
        final byte[] key = randomKey();

        // Create, fill and remove storage
        final File data = Files.createTempDirectory();
        try (final KVDao dao = KVDaoFactory.create(data)) {
            dao.upsert(key, randomValue());
        } finally {
            Files.recursiveDelete(data);
        }

        // Check that the storage is empty
        assertFalse(data.exists());
        assertTrue(data.mkdir());
        try(final KVDao dao = KVDaoFactory.create(data)) {
            assertThrows(NoSuchElementException.class, () -> dao.get(key));
        } finally {
            Files.recursiveDelete(data);
        }
    }

    @Test
    void reopen() throws IOException {
        // Reference value
        final byte[] key = randomKey();
        final byte[] value = randomValue();
        final File data = Files.createTempDirectory();
        try (KVDao dao = KVDaoFactory.create(data)) {
            // Create, fill and close storage
            dao.upsert(key, value);
        }
        // Recreate dao
        try (KVDao dao = KVDaoFactory.create(data)) {
            assertArrayEquals(value, dao.get(key));
        } finally {
            Files.recursiveDelete(data);
        }
    }
}
