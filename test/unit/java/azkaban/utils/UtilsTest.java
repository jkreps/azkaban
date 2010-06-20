package azkaban.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;
import java.util.zip.ZipFile;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

import azkaban.common.utils.Utils;

public class UtilsTest {

    @Test
    public void unzipFiles() throws Exception {
        String[] fileNames = { "blah", "a/b", "a/c", "a/d/f" };
        File testDir = createTestFiles(fileNames);
        File zip = File.createTempFile("junit-zip-test", ".zip");
        zip.deleteOnExit();
        Utils.zip(testDir, zip);
        File unzipped = Utils.createTempDir();
        Utils.unzip(new ZipFile(zip), unzipped);
        File baseOutput = unzipped.listFiles()[0];
        for(String name: fileNames)
            assertTrue("File contents not equal for " + name,
                       FileUtils.contentEquals(new File(testDir, name), new File(baseOutput, name)));
        FileUtils.deleteDirectory(unzipped);
        FileUtils.deleteDirectory(testDir);
    }

    public File createTestFiles(String... files) throws IOException {
        Random rand = new Random();
        File f = Utils.createTempDir();
        for(String s: files) {
            File newFile = new File(f, s);
            newFile.getParentFile().mkdirs();
            int size = rand.nextInt(5 * 1024);
            byte[] bytes = new byte[size];
            rand.nextBytes(bytes);
            OutputStream out = new FileOutputStream(newFile);
            out.write(bytes);
            out.close();
        }
        return f;
    }

}
