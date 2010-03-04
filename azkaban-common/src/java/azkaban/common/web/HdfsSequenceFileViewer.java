package azkaban.common.web;

import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

public abstract class HdfsSequenceFileViewer implements HdfsFileViewer {

    protected abstract boolean canReadFile(SequenceFile.Reader reader);

    protected abstract void displaySequenceFile(SequenceFile.Reader reader,
                                                PrintWriter output,
                                                int startLine,
                                                int endLine) throws IOException;

    public boolean canReadFile(FileSystem fs, Path file) {
        boolean result = false;
        try {
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, new Configuration());
            result = canReadFile(reader);
            reader.close();
        } catch(IOException e) {
            return false;
        }

        return result;
    }

    public void displayFile(FileSystem fs, Path file, PrintWriter output, int startLine, int endLine)
            throws IOException {
        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(fs, file, new Configuration());
            displaySequenceFile(reader, output, startLine, endLine);
        } catch(IOException e) {
            output.write("Error opening sequence file " + e);
        } finally {
            if(reader != null) {
                reader.close();
            }
        }
    }
}