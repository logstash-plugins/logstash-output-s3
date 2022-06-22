package org.logstash.plugins.outputs.s3;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Ruby Gzip interfaces (such as GzipReader) have limitations to access and manage the ZIP files.
 * Where Java has a deep control over ZIP files.
 * This class introduces GZIP related operations.
 */
public class GzipUtil {

    private final static int FILE_READ_SIZE = 1024;

    private final static String CORRUPTED_FILE_ERROR = "Unexpected end of ZLIB input stream";

    private final static Logger logger = LogManager.getLogger(GzipUtil.class);

    /**
     * Compresses a given non-compressed source file into compressed target file.
     * @param sourcePath a path where source is located
     * @param targetPath a path where target can be placed
     * Note: raises an IO exception if any break happens.
     */
    public static void compress(String sourcePath, String targetPath) {
        try {
            GzipUtil.compress(Paths.get(sourcePath), Paths.get(targetPath));
        } catch (IOException exception) {
            logger.error("Error occurred while compressing the file, error={}", exception.getMessage());
        }
    }

    /**
     * Decompresses and recovers corrupted GZIP file into target file.
     * @param sourcePath a path where corrupted GZIP is located
     * @param targetPath a path where target can be placed
     * Note: raises an IO exception other than GZIP dead blocks.
     */
    public static void recover(String sourcePath, String targetPath) {
        try {
            GzipUtil.decompress(Paths.get(sourcePath), Paths.get(targetPath));
        } catch (IOException exception) {
            // raise an exception if expected exception is not end of ZLIB related.
            if (CORRUPTED_FILE_ERROR.equals(exception.getMessage()) == false) {
                logger.error("Error occurred while compressing the file, error={}", exception.getMessage());
            } else {
                logger.warn("Corrupted file recovered, path:" + targetPath);
            }
        }
    }

    /**
     * Decompresses a given compressed source file into target file.
     * @param sourcePath a path where compressed source is located
     * @param targetPath a path where target can be placed
     * Note: raises an IO exception if any break happens.
     */
    public static void decompress(String sourcePath, String targetPath) {
        try {
            GzipUtil.decompress(Paths.get(sourcePath), Paths.get(targetPath));
        } catch (IOException exception) {
            logger.error("Error occurred while decompressing the file, error={}", exception.getMessage());
        }
    }

    private static void compress(Path source, Path target) throws IOException {
        try (GZIPOutputStream outputStream = new GZIPOutputStream(new FileOutputStream(target.toFile()));
             FileInputStream inputStream = new FileInputStream(source.toFile())) {

            byte[] buffer = new byte[FILE_READ_SIZE];
            int len;
            while ((len = inputStream.read(buffer)) > 0) {
                outputStream.write(buffer, 0, len);
            }
        }
    }

    private static void decompress(Path source, Path target) throws IOException {
        FileInputStream inputStream = new FileInputStream(source.toFile());
        try (GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream);
             FileOutputStream outputStream = new FileOutputStream(target.toFile())) {
            byte[] buffer = new byte[FILE_READ_SIZE];
            int len;
            while ((len = gzipInputStream.read(buffer)) > 0) {
                outputStream.write(buffer, 0, len);
            }
        }
    }
}
