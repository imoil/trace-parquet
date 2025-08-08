package com.samsung.ees.infra.api.dataprovider.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

/**
 * Utility class for handling GZIP decompression.
 */
public final class GzipUtil {
    static final int BUFFER_SIZE = 8192;

    private GzipUtil() {
        // Private constructor to prevent instantiation
    }

    public static byte[] gzipDecompress(byte[] compressedData) throws IOException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(compressedData);
             GZIPInputStream gis = new GZIPInputStream(bis);
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

            byte[] buffer = new byte[BUFFER_SIZE];
            int len;
            while ((len = gis.read(buffer)) != -1) {
                baos.write(buffer, 0, len);
            }
            return baos.toByteArray();
        }
    }

    public static String gzipDecompString(byte[] compressedData) throws IOException {
        return new String(gzipDecompress(compressedData), StandardCharsets.UTF_8);
    }
}
