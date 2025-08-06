package com.samsung.ees.infra.api.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

/**
 * Utility class for handling GZIP decompression.
 */
public final class GzipUtil {

    private GzipUtil() {
        // Private constructor to prevent instantiation
    }

    /**
     * Decompresses a GZIP byte array into a String.
     *
     * @param compressedData The GZIP compressed byte array.
     * @return The decompressed string.
     * @throws IOException if an I/O error occurs during decompression.
     */
    public static String decompress(byte[] compressedData) throws IOException {
        if (compressedData == null || compressedData.length == 0) {
            return "";
        }

        try (ByteArrayInputStream bis = new ByteArrayInputStream(compressedData);
             GZIPInputStream gis = new GZIPInputStream(bis);
             InputStreamReader reader = new InputStreamReader(gis, StandardCharsets.UTF_8);
             BufferedReader bufferedReader = new BufferedReader(reader)) {

            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                sb.append(line);
            }
            return sb.toString();
        }
    }
}
