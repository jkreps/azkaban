/*
 * Copyright 2010 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.common.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.StringWriter;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;
import org.apache.commons.io.IOUtils;

/**
 * Some helper functions
 * 
 * @author jkreps
 * 
 */
public class Utils {

    public static final Random RANDOM = new Random();
    private static Pattern WHITESPACE = Pattern.compile("\\s+");

    public static String[] split(String str) {
        return WHITESPACE.split(str, -1);
    }

    /**
     * Print the message and then exit with the given exit code
     * 
     * @param message The message to print
     * @param exitCode The exit code
     */
    public static void croak(String message, int exitCode) {
        System.err.println(message);
        System.exit(exitCode);
    }

    /**
     * Load the class
     * 
     * @param className The name of the class to load
     * @return The loaded class
     */
    public static Class<?> loadClass(String className) {
        try {
            return Class.forName(className);
        } catch(ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Load the class
     * 
     * @param className The name of the class to load
     * @return The loaded class
     */
    public static Class<?> loadClass(String className, ClassLoader loader) {
        try {
            return loader.loadClass(className);
        } catch(ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Get an annotation from the class
     * 
     * @param <T> The Annotation type
     * @param theClass The class to read the annotation from
     * @param annotation The annotation to read
     * @return The annotation if it is there
     * @throws IllegalArgumentException if the Annotation is not present
     */
    public static <T extends Annotation> T getRequiredAnnotation(Class<?> theClass,
                                                                 Class<T> annotation) {
        T t = theClass.getAnnotation(annotation);
        if(t == null)
            throw new IllegalArgumentException("The expected annotation '" + annotation.getName()
                                               + "' was not found on class '" + theClass.getName()
                                               + "'.");
        return t;
    }

    /**
     * Get the named method from the class
     * 
     * @param c The class to get the method from
     * @param name The method name
     * @param argTypes The argument types
     * @return The method
     */
    public static Method getMethod(Class<?> c, String name, Class<?>... argTypes) {
        try {
            return c.getMethod(name, argTypes);
        } catch(NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Return the object if it is non-null, otherwise throw an exception
     * 
     * @param <T> The type of the object
     * @param t The object
     * @return The object if it is not null
     * @throws IllegalArgumentException if the object is null
     */
    public static <T> T nonNull(T t) {
        if(t == null)
            throw new IllegalArgumentException("Null value not allowed.");
        else
            return t;
    }

    /**
     * Get the Class of all the objects
     * 
     * @param args The objects to get the Classes from
     * @return The classes as an array
     */
    public static Class<?>[] getTypes(Object... args) {
        Class<?>[] argTypes = new Class<?>[args.length];
        for(int i = 0; i < argTypes.length; i++)
            argTypes[i] = args[i].getClass();
        return argTypes;
    }

    /**
     * Call the named method
     * 
     * @param obj The object to call the method on
     * @param c The class of the object
     * @param name The name of the method
     * @param args The method arguments
     * @return The result of the method
     */
    public static Object callMethod(Object obj, Class<?> c, String name, Object... args) {
        return callMethod(obj, c, name, getTypes(args), args);
    }

    /**
     * Call the named method
     * 
     * @param obj The object to call the method on
     * @param c The class of the object
     * @param name The name of the method
     * @param args The method arguments
     * @return The result of the method
     */
    public static Object callMethod(Object obj,
                                    Class<?> c,
                                    String name,
                                    Class<?>[] classes,
                                    Object[] args) {
        try {
            Method m = getMethod(c, name, classes);
            return m.invoke(obj, args);
        } catch(InvocationTargetException e) {
            throw getCause(e);
        } catch(IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Call the class constructor with the given arguments
     * 
     * @param className The name of the class
     * @param args The arguments
     * @return The constructed object
     */
    public static Object callConstructor(String className, Object... args) {
        return callConstructor(Utils.loadClass(className), getTypes(args), args);
    }

    /**
     * Call the class constructor with the given arguments
     * 
     * @param c The class
     * @param args The arguments
     * @return The constructed object
     */
    public static Object callConstructor(Class<?> c, Object... args) {
        return callConstructor(c, getTypes(args), args);
    }

    /**
     * Call the class constructor with the given arguments
     * 
     * @param c The class
     * @param args The arguments
     * @return The constructed object
     */
    public static Object callConstructor(Class<?> c, Class<?>[] argTypes, Object[] args) {
        try {
            Constructor<?> cons = c.getConstructor(argTypes);
            return cons.newInstance(args);
        } catch(InvocationTargetException e) {
            throw getCause(e);
        } catch(IllegalAccessException e) {
            throw new IllegalStateException(e);
        } catch(NoSuchMethodException e) {
            throw new IllegalStateException(e);
        } catch(InstantiationException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Returns true if the constructor with the given args exists.
     * 
     * @param c The class
     * @param args The arguments
     * @return
     */
    public static boolean constructorExist(Class<?> c, Object... args) {
        Class<?>[] argTypes = getTypes(args);
        try {
            @SuppressWarnings("unused")
            Constructor<?> cons = c.getConstructor(argTypes);
        } catch(NoSuchMethodException e) {
            return false;
        }

        return true;
    }

    /**
     * @return True if it is null or empty
     */
    public static boolean isNullOrEmpty(String s) {
        return s == null || "".equals(s);
    }

    /**
     * Get the root cause of the Exception
     * 
     * @param e The Exception
     * @return The root cause of the Exception
     */
    private static RuntimeException getCause(InvocationTargetException e) {
        Throwable cause = e.getCause();
        if(cause instanceof RuntimeException)
            throw (RuntimeException) cause;
        else
            throw new IllegalStateException(e.getCause());
    }

    /**
     * Create a directory at the path given (if there isn't already one there)
     * 
     * @param path The path to create
     */
    public static void makePaths(File path) {
        if(!path.getParentFile().exists())
            makePaths(path.getParentFile());

        if(!path.exists())
            path.mkdir();
    }

    public static <T> List<T> sorted(List<T> l, Comparator<T> comparator) {
        List<T> copy = new ArrayList<T>(l);
        Collections.sort(copy, comparator);
        return copy;
    }

    public static <T extends Comparable<? super T>> List<T> sorted(List<T> l) {
        List<T> copy = new ArrayList<T>(l);
        Collections.sort(copy);
        return copy;
    }

    public static File[] ls(String dir) {
        return ls(new File(dir));
    }

    public static File[] ls(File dir) {
        if(!dir.exists() || !dir.isDirectory() || !dir.canRead())
            throw new IllegalArgumentException(dir
                                               + " is not a readable directory or does not exist.");

        File[] files = dir.listFiles();
        if(files == null)
            return new File[0];
        else
            return files;
    }

    public static List<String> getClassLoaderDescriptions(ClassLoader loader) {
        List<String> values = new ArrayList<String>();
        while(loader != null) {
            if(loader instanceof URLClassLoader) {
                URLClassLoader urlLoader = (URLClassLoader) loader;
                for(URL url: urlLoader.getURLs())
                    values.add(url.toString());
            } else {
                values.add(loader.getClass().getName());
            }
            loader = loader.getParent();
        }
        return values;
    }

    public static String stackTrace(Throwable t) {
        if(t == null) {
            return "Utils.stackTrace(...) -- Throwable was null.";
        }
        StringWriter writer = new StringWriter();
        t.printStackTrace(new PrintWriter(writer));
        return writer.toString();
    }

    public static void zip(File input, File output) throws IOException {
        FileOutputStream out = new FileOutputStream(output);
        ZipOutputStream zOut = new ZipOutputStream(out);
        zipFile("", input, zOut);
        zOut.close();
    }

    private static void zipFile(String path, File input, ZipOutputStream zOut) throws IOException {
        if(input.isDirectory()) {
            File[] files = input.listFiles();
            if(files != null) {
                for(File f: files) {
                    String childPath = path + input.getName() + (f.isDirectory() ? "/" : "");
                    zipFile(childPath, f, zOut);
                }
            }
        } else {
            String childPath = path + (path.length() > 0 ? "/" : "") + input.getName();
            ZipEntry entry = new ZipEntry(childPath);
            zOut.putNextEntry(entry);
            InputStream fileInputStream = new BufferedInputStream(new FileInputStream(input));
            IOUtils.copy(fileInputStream, zOut);
            fileInputStream.close();
        }
    }

    public static void unzip(ZipFile source, File dest) throws IOException {
        Enumeration<?> entries = source.entries();
        while(entries.hasMoreElements()) {
            ZipEntry entry = (ZipEntry) entries.nextElement();
            File newFile = new File(dest, entry.getName());
            if(entry.isDirectory()) {
                newFile.mkdirs();
            } else {
                newFile.getParentFile().mkdirs();
                InputStream src = source.getInputStream(entry);
                OutputStream output = new BufferedOutputStream(new FileOutputStream(newFile));
                IOUtils.copy(src, output);
                src.close();
                output.close();
            }
        }
    }

    public static File createTempDir() {
        return createTempDir(new File(System.getProperty("java.io.tmpdir")));
    }

    public static File createTempDir(File parent) {
        File temp = new File(parent, Integer.toString(Math.abs(RANDOM.nextInt()) % 100000000));
        temp.delete();
        temp.mkdir();
        temp.deleteOnExit();
        return temp;
    }

    /**
     * Read in content of a file and get the last *lineCount* lines. It is
     * equivalent to *tail* command
     * 
     * @param filename           input file name
     * @param lineCount          desired number of tailing lines
     * @return vector of the last *lineCount* lines
     */
    public static Vector<String> tail(String filename, int lineCount) {
        return tail(filename, lineCount, 2000);
    }

    /**
     * Read in content of a file and get the last *lineCount* lines. It is
     * equivalent to *tail* command
     * 
     * @param filename
     * @param lineCount
     * @param chunkSize
     * @return
     */
    public static Vector<String> tail(String filename, int lineCount, int chunkSize) {
        try {
            // read in content of the file
            RandomAccessFile file = new RandomAccessFile(filename, "r");
            // destination vector
            Vector<String> lastNLines = new Vector<String>();
            // current position
            long currPos = file.length() - 1;
            long startPos;
            byte[] byteArray = new byte[chunkSize];

            // read in content of the file in reverse order
            while(true) {
                // read in from *fromPos*
                startPos = currPos - chunkSize;

                if(startPos <= 0) { // toward the beginning of the file
                    file.seek(0);
                    file.read(byteArray, 0, (int) currPos); // only read in
                                                            // curPos bytes
                    parseLinesFromLast(byteArray, 0, (int) currPos, lineCount, lastNLines);
                    break;
                } else {
                    file.seek(startPos);
                    if(byteArray == null)
                        byteArray = new byte[chunkSize];
                    file.readFully(byteArray);
                    if(parseLinesFromLast(byteArray, lineCount, lastNLines)) {
                        break; // we got the last *lineCount* lines
                    }

                    // move the current position
                    currPos = startPos; // + lastLine.getBytes().length;
                }
            }

            // there might be lineCount + 1 lines and the first line (now it is the last line)
            // might not be complete
            for (int index= lineCount; index < lastNLines.size(); index++)
                lastNLines.removeElementAt(index);
            
            // reverse the order of elements in lastNLines
            Collections.reverse(lastNLines);
            return lastNLines;
        } catch(Exception e) {
            return null;
        }

    }

    /**
     * Parse lines in byteArray and store the last *lineCount* lines in
     * *lastNLines*
     * 
     * @param byteArray                 source byte array
     * @param lineCount                   desired number of lines
     * @param lastNLines                vector of last N lines
     * @return true     indicates we get *lineCount* lines 
     *                false     otherwise
     */

    protected static boolean parseLinesFromLast(byte[] byteArray,
                                                int lineCount,
                                                Vector<String> lastNLines) {
        return parseLinesFromLast(byteArray, 0, byteArray.length, lineCount, lastNLines);
    }

    /**
     * Parse lines in byteArray and store the last *lineCount* lines in
     * *lastNLines*
     * 
     * @param byteArray         source byte array
     * @param offset                offset of the byte array
     * @param length                length of the byte array
     * @param lineCount             desired number of lines
     * @param lastNLines        vector of last N lines
     * @return true         indicates we get *lineCount* lines 
     *                false         otherwise
     */
    protected static boolean parseLinesFromLast(byte[] byteArray,
                                                int offset,
                                                int length,
                                                int lineCount,
                                                Vector<String> lastNLines) {

        if(lastNLines.size() > lineCount)
            return true;

        // convert byte array to string
        String lastNChars = new String(byteArray, offset, length);

        // reverse the string
        StringBuffer sb = new StringBuffer(lastNChars);
        lastNChars = sb.reverse().toString();

        // tokenize the string using "\n"
        String[] tokens = lastNChars.split("\n");

        // append lines to lastNLines
        for (int index=0; index < tokens.length; index++) {
            StringBuffer sbLine = new StringBuffer(tokens[index]);
            String newline = sbLine.reverse().toString();
            
            if (index == 0 && !lastNLines.isEmpty()) { // first line might not be a complete line
                int lineNum = lastNLines.size();
                String halfLine = lastNLines.get(lineNum - 1);
                lastNLines.set(lineNum - 1, newline + halfLine);
            }
            else {
                lastNLines.add(newline);
            }
            
            if(lastNLines.size() > lineCount) {
                return true;
            }
        }

        return false;
    }
}
