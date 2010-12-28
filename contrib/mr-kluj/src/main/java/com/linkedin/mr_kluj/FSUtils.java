package com.linkedin.mr_kluj;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 */
public class FSUtils {

    public static List<FileStatus> spiderPath(FileSystem fs, Path path) throws IOException {
        LinkedList<FileStatus> retVal = new LinkedList<FileStatus>();

        for (FileStatus fileStatus : fs.listStatus(path)) {
            if (fileStatus.isDir()) {
                retVal.addAll(spiderPath(fs, fileStatus.getPath()));
            }
            else {
                retVal.add(fileStatus);
            }
        }

        return retVal;
    }
}
