package com.samsung.ees.infra.api.dataprovider.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPOutputStream;

/**
 * Utility class for tests.
 */
public class TestUtils {
    /**
     * Compresses a string into a GZIP byte array.
     *
     * @param content The string content to compress.
     * @return GZIP compressed byte array.
     * @throws IOException if an I/O error has occurred.
     */
    public static byte[] createGzipData(String content) throws IOException {
        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
             GZIPOutputStream gzipStream = new GZIPOutputStream(byteStream)) {
            gzipStream.write(content.getBytes(StandardCharsets.UTF_8));
            return byteStream.toByteArray();
        }
    }
}
