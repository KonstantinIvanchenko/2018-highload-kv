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

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Utility classes for handling files
 *
 * @author Vadim Tsesko <incubos@yandex.com>
 */
final class Files {
    private static final String TEMP_PREFIX = "highload-kv";
    private static final String TEMP_PREFIX_MEDIA = "/media/konstiva/My Book/temp";

    private Files() {
        // Don't instantiate
    }

    static File createTempDirectory() throws IOException {
        //final File data = java.nio.file.Files.createTempDirectory(TEMP_PREFIX).toFile();
        final File data = java.nio.file.Files.createTempDirectory(Paths.get(TEMP_PREFIX_MEDIA), TEMP_PREFIX).toFile();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                if (data.exists()) {
                    recursiveDelete(data);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));
        return data;
    }

    static void recursiveDelete(@NotNull final File path) throws IOException {
        java.nio.file.Files.walkFileTree(
                path.toPath(),
                new SimpleFileVisitor<Path>() {
                    private void remove(@NotNull final Path file) throws IOException {
                        if (!file.toFile().delete()) {
                            throw new IOException("Can't delete " + file);
                        }
                    }

                    @NotNull
                    @Override
                    public FileVisitResult visitFile(
                            @NotNull final Path file,
                            @NotNull final BasicFileAttributes attrs) throws IOException {
                        remove(file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                        remove(dir);
                        return FileVisitResult.CONTINUE;
                    }
                });
    }
}
