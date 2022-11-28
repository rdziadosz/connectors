package io.delta.flink.e2e.utils;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;

public final class TestHelper {

    private static final TestHelper INSTANCE = new TestHelper();

    /**
     * Returns the contents of the resource file as a String.
     *
     * @param pathToFile path to resource file
     * @return file content
     */
    public static String readTestFile(String pathToFile) {
        try {
            URI uri = Objects.requireNonNull(INSTANCE.getClass().getResource(pathToFile)).toURI();
            return new String(Files.readAllBytes(Paths.get(uri)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}