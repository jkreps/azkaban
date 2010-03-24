package azkaban.jobs;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import azkaban.app.JobDescriptor;

public class JavaProcessJob extends ProcessJob {
	public static final String CLASSPATH = "classpath";
	public static final String JAVA_CLASS = "java.class";
	public static final String INITIAL_MEMORY_SIZE = "Xms";
	public static final String MAX_MEMORY_SIZE = "Xms";
	public static final String MAIN_ARGS = "main.args";
	public static final String JVM_PARAMS = "jvm.args";

	public static final String DEFAULT_INITIAL_MEMORY_SIZE = "64M";
	public static final String DEFAULT_MAX_MEMORY_SIZE = "256M";

	public static String JAVA_COMMAND = "java";

	public JavaProcessJob(JobDescriptor descriptor) {
		super(descriptor);
	}

	@Override
	protected List<String> getCommandList() {
		ArrayList<String> list = new ArrayList<String>();
		list.add(createCommandLine());
		return list;
	}

	protected String createCommandLine() {
		String command = JAVA_COMMAND + " ";
		command += getJVMArguments() + " ";
		command += "-Xms" + getInitialMemorySize() + " ";
		command += "-Xmx" + getMaxMemorySize() + " ";
		command += "-cp " + createArguments(getClassPaths(), ":") + " ";
		command += getJavaClass() + " ";
		command += createArguments(getMainArguments(), " ");

		return command;
	}

	protected String getJavaClass() {
		return getProps().getString(JAVA_CLASS);
	}

	protected String getClassPathParam() {
		List<String> classPath = getClassPaths();
		if (classPath == null || classPath.size() == 0) {
			return "";
		}

		return "-cp " + createArguments(classPath, ":") + " ";
	}

	protected List<String> getClassPaths() {
		List<String> classPaths = getProps()
				.getStringList(CLASSPATH, null, ",");

		if (classPaths == null) {
			File path = new File(getPath());
			File parent = path.getParentFile();
			classPaths = new ArrayList<String>();
			for (File file : parent.listFiles()) {
				if (file.getName().endsWith(".jar")) {
					classPaths.add(file.getName());
				}
			}
		}

		return classPaths;
	}

	protected String getInitialMemorySize() {
		return getProps().getString(INITIAL_MEMORY_SIZE,
				DEFAULT_INITIAL_MEMORY_SIZE);
	}

	protected String getMaxMemorySize() {
		return getProps().getString(MAX_MEMORY_SIZE, DEFAULT_MAX_MEMORY_SIZE);
	}

	protected List<String> getMainArguments() {
		return getProps().getStringList(MAIN_ARGS, null, ",");
	}

	protected String getJVMArguments() {
		return getProps().getString(JVM_PARAMS, "");
	}

	protected String createArguments(List<String> arguments, String separator) {
		if (arguments != null && arguments.size() > 0) {
			String param = "";
			for (String arg : arguments) {
				param += arg + separator;
			}

			return param.substring(0, param.length() - 1);
		}

		return "";
	}

}
