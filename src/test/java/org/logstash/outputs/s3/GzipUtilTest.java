package org.logstash.outputs.s3;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.After;
import org.junit.Test;
import org.logstash.plugins.outputs.s3.GzipUtil;

import static org.junit.Assert.assertTrue;

public class GzipUtilTest {

    private final static String RESOURCES_PATH = "src/test/resources/";
    private final static String ABSOLUTE_PATH = new File(RESOURCES_PATH).getAbsolutePath();
    private final static String OUTPUT_ZIP_FILE = ABSOLUTE_PATH + "/output.txt.gz";
    private final static String OUTPUT_TXT_FILE = ABSOLUTE_PATH + "/output.txt";
    private final static Path OUTPUT_ZIP_FILE_PATH = Paths.get(OUTPUT_ZIP_FILE);
    private final static Path OUTPUT_TXT_FILE_PATH = Paths.get(OUTPUT_TXT_FILE);

    @After
    public void cleanUp() throws IOException {
        if (Files.exists(OUTPUT_ZIP_FILE_PATH)) {
            Files.delete(OUTPUT_ZIP_FILE_PATH);
        }
        if (Files.exists(Paths.get(OUTPUT_TXT_FILE))) {
            Files.delete(Paths.get(OUTPUT_TXT_FILE));
        }
        System.setOut(System.out);
    }

    @Test
    public void testCompressFailure() {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outputStream));

        GzipUtil.compress("", "");
        String expected = "No such file or directory";

        assertTrue(outputStream.toString().contains(expected));
    }

    @Test
    public void testCompressSuccess() {
        GzipUtil.compress(ABSOLUTE_PATH + "/test.txt", OUTPUT_ZIP_FILE);
        assertTrue(Files.exists(OUTPUT_ZIP_FILE_PATH));
    }

    @Test
    public void testRecover() {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outputStream));

        GzipUtil.recover(ABSOLUTE_PATH + "/corrupted.txt.gz", OUTPUT_TXT_FILE);
        assertTrue(Files.exists(OUTPUT_TXT_FILE_PATH));

        // end of ZLIB error is filtered out
        assertTrue(outputStream.toString().isEmpty());
    }

    @Test
    public void testDecompressSuccess() {
        GzipUtil.decompress(ABSOLUTE_PATH + "/healthy.txt.gz", OUTPUT_TXT_FILE);
        assertTrue(Files.exists(OUTPUT_TXT_FILE_PATH));
    }
}
