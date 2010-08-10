package azkaban;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;

public class TestUtils {

    public static File writeString(String s, String fileSuffix) {
        try {
            File f = File.createTempFile("azktest", fileSuffix);
            f.deleteOnExit();
            FileOutputStream output = new FileOutputStream(f);
            IOUtils.write(s, output);
            IOUtils.closeQuietly(output);
            return f;
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }
    
}
