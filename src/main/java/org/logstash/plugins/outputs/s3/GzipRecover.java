package org.logstash.plugins.outputs.s3;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.GZIPInputStream;

public class GzipRecover {

    private final static int FILE_READ_SIZE = 1024;

    public static void decompressGzip(String sourcePath, String targetSource) {
        try {
            GzipRecover.decompressGzip(Paths.get(sourcePath), Paths.get(targetSource));
        } catch (IOException e) {
            // Unexpected end of ZLIB input stream
            e.printStackTrace();
        }
    }

    private static void decompressGzip(Path source, Path target) throws IOException {
        FileInputStream inputStream = new FileInputStream(source.toFile());
        int size = inputStream.available();

        try (GZIPInputStream gis = new GZIPInputStream(inputStream);
             FileOutputStream fos = new FileOutputStream(target.toFile())) {
            byte[] buffer = new byte[FILE_READ_SIZE];
            int len;
            while ((len = gis.read(buffer)) > 0) {
                System.out.println(len);
                fos.write(buffer, 0, len);
            }
        }
    }
}
