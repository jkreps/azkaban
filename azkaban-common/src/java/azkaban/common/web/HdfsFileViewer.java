package azkaban.common.web;

import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public interface HdfsFileViewer {

    public boolean canReadFile(FileSystem fs, Path path);

    public void displayFile(FileSystem fs,
                            Path path,
                            PrintWriter output,
                            int startLine,
                            int endLine) throws IOException;

}