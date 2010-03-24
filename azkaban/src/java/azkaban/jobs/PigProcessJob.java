package azkaban.jobs;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import azkaban.app.JobDescriptor;

public class PigProcessJob extends JavaProcessJob {
	public static final String PIG_SCRIPT = "pig.script";
	public static final String UDF_IMPORT = "Dudf.import.list";
	public static final String PIG_PARAM_PREFIX = "param.";
	public static final String PIG_PARAM_FILES = "paramfile";
	public static final String DEBUG = "debug";

	public static final String PIG_JAVA_CLASS = "org.apache.pig.Main";

	public PigProcessJob(JobDescriptor descriptor) {
		super(descriptor);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected String getJavaClass() {
		return PIG_JAVA_CLASS;
	}

	@Override
	protected String getJVMArguments() {
		String args = super.getJVMArguments();

		List<String> udfImport = getUDFImportList();
		if (udfImport == null) {
			return args;
		}

		args += " -Dudf.import.list=" + super.createArguments(udfImport, ":");

		return args;
	}

	@Override
	protected List<String> getMainArguments() {
		ArrayList<String> list = new ArrayList<String>();
		Map<String, String> map = getPigParams();
		if (map != null) {
			for (Map.Entry<String, String> entry : map.entrySet()) {
				list.add("-param " + entry.getKey() + "=" + entry.getValue());
			}
		}

		List<String> paramFiles = getPigParamFiles();
		if (paramFiles != null) {
			for (String paramFile : paramFiles) {
				list.add("-param_file " + paramFile);
			}
		}

		if (getDebug()) {
			list.add("-debug");
		}

		list.add(getScript());

		return list;
	}

	@Override
	protected List<String> getClassPaths() {
		List<String> classPath = super.getClassPaths();

		// Add hadoop home setting.
		String hadoopHome = System.getenv("HADOOP_HOME");
		if (hadoopHome == null) {
			info("HADOOP_HOME not set, using default hadoop config.");
		} else {
			info("Using hadoop config found in " + hadoopHome);
			classPath.add(new File(hadoopHome, "conf").getPath());
		}

		return classPath;
	}

	protected boolean getDebug() {
		return getProps().getBoolean(DEBUG, false);
	}

	protected String getScript() {
		return getProps().getString(PIG_SCRIPT);
	}

	protected List<String> getUDFImportList() {
		return getProps().getStringList(UDF_IMPORT, null, ",");
	}

	protected Map<String, String> getPigParams() {
		return super.getMapFromPrefixProperties(PIG_PARAM_PREFIX);
	}

	protected List<String> getPigParamFiles() {
		return getProps().getStringList(PIG_PARAM_FILES, null, ",");
	}
}
