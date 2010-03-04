package azkaban.app;

import java.io.File;
import java.io.IOException;
import java.util.List;

import azkaban.common.utils.Props;

public class PropsUtils {

    /**
     * Load job schedules from the given directories ] * @param dir The
     * directory to look in
     * 
     * @param suffixes File suffixes to load
     * @return The loaded set of schedules
     */
    public static Props loadPropsInDir(File dir, String... suffixes) {
        return loadPropsInDir(null, dir, suffixes);
    }

    /**
     * Load job schedules from the given directories
     * 
     * @param parent The parent properties for these properties
     * @param dir The directory to look in
     * @param suffixes File suffixes to load
     * @return The loaded set of schedules
     */
    public static Props loadPropsInDir(Props parent, File dir, String... suffixes) {
        try {
            Props props = new Props(parent);
            File[] files = dir.listFiles();
            if(files != null) {
                for(File f: files) {
                    if(f.isFile() && endsWith(f, suffixes)) {
                        props.putAll(new Props(null, f.getAbsolutePath()));
                    }
                }
            }
            return props;
        } catch(IOException e) {
            throw new RuntimeException("Error loading properties.", e);
        }
    }

    /**
     * Load job schedules from the given directories
     * 
     * @param dirs The directories to check for properties
     * @param suffixes The suffixes to load
     * @return The properties
     */
    public static Props loadPropsInDirs(List<File> dirs, String... suffixes) {
        Props props = new Props();
        for(File dir: dirs) {
            props.putLocal(loadPropsInDir(dir, suffixes));
        }
        return props;
    }

    /**
     * Load properties from the given path
     * 
     * @param jobPath The path to load from
     * @param props The parent properties for loaded properties
     * @param suffixes The suffixes of files to load
     */
    public static void loadPropsBySuffix(File jobPath, Props props, String... suffixes) {
        try {
            if(jobPath.isDirectory()) {
                File[] files = jobPath.listFiles();
                if(files != null) {
                    for(File file: files)
                        loadPropsBySuffix(file, props, suffixes);
                }
            } else if(endsWith(jobPath, suffixes)) {
                props.putAll(new Props(null, jobPath.getAbsolutePath()));
            }
        } catch(IOException e) {
            throw new RuntimeException("Error loading schedule properties.", e);
        }
    }

    public static boolean endsWith(File file, String... suffixes) {
        for(String suffix: suffixes)
            if(file.getName().endsWith(suffix))
                return true;
        return false;
    }
}
