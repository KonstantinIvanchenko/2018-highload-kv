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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Functional unit tests for {@link KVDao} implementations
 *
 * @author Vadim Tsesko <incubos@yandex.com>
 */
class KVDaoTest extends TestBase {
    private static File data;
    private static KVDao dao;

    @BeforeAll
    static void beforeAll() throws IOException {
        data = Files.createTempDirectory();
        dao = KVDaoFactory.create(data);
    }

    @AfterAll
    static void afterAll() throws IOException {
        dao.close();
        Files.recursiveDelete(data);
    }

    @Test
    void empty() {
        assertThrows(NoSuchElementException.class, () -> dao.get(randomKey()));
    }

    @Test
    void nonUnicode() throws IOException {
        // Different byte arrays
        final byte[] a1 = new byte[]{
                (byte) 0xD0, (byte) 0x9F, // 'П'
                (byte) 0xD1, (byte) 0x80, // 'р'
                (byte) 0xD0,              // corrupted UTF-8, was 'и'
                (byte) 0xD0, (byte) 0xB2, // 'в'
                (byte) 0xD0, (byte) 0xB5, // 'е'
                (byte) 0xD1, (byte) 0x82  // 'т'
        };
        final byte[] a2 = new byte[]{
                (byte) 0xD0, (byte) 0x9F, // 'П'
                (byte) 0xD1, (byte) 0x80, // 'р'
                (byte) 0xD1,              // corrupted UTF-8, was 'и'
                (byte) 0xD0, (byte) 0xB2, // 'в'
                (byte) 0xD0, (byte) 0xB5, // 'е'
                (byte) 0xD1, (byte) 0x82  // 'т'
        };
        assertFalse(Arrays.equals(a1, a2));

        // But same strings
        assertArrayEquals(new String(a1, StandardCharsets.UTF_8).getBytes(), new String(a2, StandardCharsets.UTF_8).getBytes());

        // Put a1 value
        final byte[] value = randomValue();
        dao.upsert(a1, value);

        // Check that a2 value is absent
        assertThrows(NoSuchElementException.class, () -> dao.get(a2));
    }

    @Test
    void nonHash() throws IOException {
        // Different byte arrays
        final byte[] a1 = new byte[]{(byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x1F};
        final byte[] a2 = new byte[]{(byte) 0x00, (byte) 0x00, (byte) 0x01, (byte) 0x00};
        assertFalse(Arrays.equals(a1, a2));

        // But hash codes are equal
        assertEquals(Arrays.hashCode(a1), Arrays.hashCode(a2));

        // Put a1 value
        final byte[] value = randomValue();
        dao.upsert(a1, value);

        // Check that a2 value is absent
        assertThrows(NoSuchElementException.class, () -> dao.get(a2));
    }

    @Test
    void insert() throws IOException {
        final byte[] key = randomKey();
        final byte[] value = randomValue();
        dao.upsert(key, value);
        assertArrayEquals(value, dao.get(key));
        assertArrayEquals(value, dao.get(key.clone()));
    }

    @Test
    void emptyValue() throws IOException {
        final byte[] key = randomKey();
        final byte[] value = new byte[0];
        dao.upsert(key, value);
        assertArrayEquals(value, dao.get(key));
        assertArrayEquals(value, dao.get(key.clone()));
    }

    @Test
    void upsert() throws IOException {
        final byte[] key = randomKey();
        final byte[] value1 = randomValue();
        final byte[] value2 = randomValue();
        dao.upsert(key, value1);
        assertArrayEquals(value1, dao.get(key));
        assertArrayEquals(value1, dao.get(key.clone()));
        dao.upsert(key, value2);
        assertArrayEquals(value2, dao.get(key));
        assertArrayEquals(value2, dao.get(key.clone()));
    }

    @Test
    void remove() throws Exception {
        final byte[] key = randomKey();
        final byte[] value = randomValue();
        dao.upsert(key, value);
        assertArrayEquals(value, dao.get(key));
        assertArrayEquals(value, dao.get(key.clone()));
        dao.remove(key);
        assertThrows(NoSuchElementException.class, () -> dao.get(key));
    }
}
