package azkaban.examples.pig;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Reader;
import java.io.StringWriter;
import java.net.URI;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.jar.JarFile;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.impl.PigContext;

import azkaban.common.jobs.AbstractJob;
import azkaban.common.utils.Props;

public class PigRunnerJob extends AbstractJob {
	protected Props props;

	public PigRunnerJob(String name, Props props) {
		super(name);
		this.props = props;
	}

	public void run() {
		List<File> cleanupFiles = new ArrayList<File>();
		try {
			String pigScriptPath = props.get("pig.script");

			// if the pig script is not local, try and extract it from the jar
			if (!new File(pigScriptPath).exists()) {
				pigScriptPath = readPigScriptFromJarFile(pigScriptPath);
			}

			getLog().info("Starting Pig Job with script");
			getLog()
					.info(
							"***************************************************************");
			getLog().info(getScriptText(pigScriptPath));
			getLog()
					.info(
							"***************************************************************");

			Properties properties = new Properties();

			if (props.containsKey("hadoop.job.ugi")) {
				properties.put("hadoop.job.ugi", props.get("hadoop.job.ugi"));
			}

			setupDistributedLibraries(properties);

			PigContext pigContext = new PigContext(ExecType.MAPREDUCE,
					properties);
			

			// register jar files -- pig only accepts local jar files so we will
			// yank them out of hdfs
			for (String jar : props.getStringList("include.jars")) {
				getLog().info("Registering jar " + jar);
				Path jarPath = new Path(jar);
				FileSystem fs = jarPath.getFileSystem(new Configuration());
				FSDataInputStream input = fs.open(jarPath);
				String namePrefix = new File(jar).getName().split("\\.")[0];
				File localJar = File.createTempFile(namePrefix, ".jar");
				OutputStream output = new FileOutputStream(localJar);
				IOUtils.copy(input, output);
				output.close();
				input.close();
				pigContext.addJar(localJar.getAbsolutePath());
				cleanupFiles.add(localJar);
			}

			pigContext.getPackageImportList().add("com.linkedin.pig.");

			PigServer pigServer = new PigServer(pigContext);
			
			pigServer.setJobName(new Path(props.get("pig.script")).getName());
			pigServer.registerScript(pigScriptPath);

			getLog().info("pig Job completed successfully !!");
		}

		catch (Exception e) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			e.printStackTrace(new PrintStream(baos));
			getLog().error(baos.toString());
			throw new RuntimeException(e);
		} finally {
			for (File f : cleanupFiles)
				f.delete();
		}
	}

	public String readPigScriptFromJarFile(String scriptfilePath)
			throws IOException {
		String jarFileName = findContainingJar(scriptfilePath, Thread
				.currentThread().getContextClassLoader());
		if (jarFileName == null)
			throw new IllegalArgumentException("Could not find "
					+ scriptfilePath + " in your jars.");
		JarFile jarFile = new JarFile(jarFileName);

		InputStream is = jarFile.getInputStream(jarFile
				.getJarEntry(scriptfilePath));
		if (is == null) {
			throw new RuntimeException("Failed to find pig.script:"
					+ scriptfilePath + " on the classpath.");
		}
		// create a temp file to be deleted on JVM exit
		File tempFile = File.createTempFile("temp.pig", ".script");
		tempFile.deleteOnExit();

		OutputStream os = new BufferedOutputStream(new FileOutputStream(
				tempFile));
		int val = -1;
		while ((val = is.read()) != -1) {
			os.write(val);
		}
		os.write('\n');
		os.close();

		return tempFile.getAbsolutePath();
	}

	private String getScriptText(String filePath) throws IOException {
		StringWriter writer = new StringWriter();
		Reader reader = new BufferedReader(new FileReader(filePath));
		int val = -1;
		while ((val = reader.read()) != -1) {
			writer.write(val);
		}

		writer.close();
		reader.close();
		return writer.toString();
	}

	private void setupDistributedLibraries(Properties properties)
			throws IOException {
		String hadoopCacheJarDir = props.getString(
				"hdfs.default.classpath.dir", null);

		if (hadoopCacheJarDir != null) {
			Path path = new Path(hadoopCacheJarDir);
			FileSystem fs = path.getFileSystem(new Configuration());

			if (fs != null) {
				FileStatus[] status = fs
						.listStatus(new Path(hadoopCacheJarDir));

				if (status != null) {
					for (int i = 0; i < status.length; ++i) {
						if (!status[i].isDir()) {
							Path jarPath = new Path(hadoopCacheJarDir,
									status[i].getPath().getName());
							info("Registering Pig Jar:" + jarPath);
							addFileToClassPath(jarPath, properties);
						}
					}
				} else {
					info("hdfs.default.classpath.dir " + hadoopCacheJarDir
							+ " is empty.");
				}
			} else {
				info("hdfs.default.classpath.dir " + hadoopCacheJarDir
						+ " filesystem doesn't exist");
			}
		} else {
			info("hdfs.default.classpath.dir not set");
		}
	}

	/**
	 * The following code has been taken from hadoop's distributed cache to get
	 * distributed caches to work with pig.
	 * 
	 * @param uri
	 * @param props
	 */
	public static void addCacheFile(URI uri, Properties props) {
		String files = (String) props.get("mapred.cache.files");
		props.put("mapred.cache.files", files == null ? uri.toString() : files
				+ "," + uri.toString());
	}

	public static void addFileToClassPath(Path file, Properties props)
			throws IOException {
		String classpath = (String) props.get("mapred.job.classpath.files");
		props.put("mapred.job.classpath.files", classpath == null ? file
				.toString() : classpath + System.getProperty("path.separator")
				+ file.toString());
		FileSystem fs = FileSystem.get(new Configuration());
		URI uri = fs.makeQualified(file).toUri();

		addCacheFile(uri, props);
	}

	/**
	 * Find a jar that contains a class of the same name, if any. It will return
	 * a jar file, even if that is not the first thing on the class path that
	 * has a class with the same name.
	 * 
	 * @param my_class
	 *            the class to find.
	 * @return a jar file that contains the class, or null.
	 * @throws IOException
	 */
	public static String findContainingJar(Class my_class, ClassLoader loader) {
		String class_file = my_class.getName().replaceAll("\\.", "/")
				+ ".class";
		return findContainingJar(class_file, loader);
	}

	public static String findContainingJar(String fileName, ClassLoader loader) {
		try {
			for (Enumeration itr = loader.getResources(fileName); itr
					.hasMoreElements();) {
				URL url = (URL) itr.nextElement();
				if ("jar".equals(url.getProtocol())) {
					String toReturn = url.getPath();
					if (toReturn.startsWith("file:")) {
						toReturn = toReturn.substring("file:".length());
					}
					toReturn = URLDecoder.decode(toReturn, "UTF-8");
					return toReturn.replaceAll("!.*$", "");
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return null;
	}
}
