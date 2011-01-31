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

package azkaban.jobs.builtin;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import azkaban.app.JobDescriptor;
import azkaban.util.StringUtils;

public class PigProcessJob extends JavaProcessJob {
    
	public static final String PIG_SCRIPT = "pig.script";
	public static final String UDF_IMPORT = "udf.import.list";
	public static final String PIG_PARAM_PREFIX = "param.";
	public static final String PIG_PARAM_FILES = "paramfile";
	public static final String HADOOP_UGI = "hadoop.job.ugi";
	public static final String DEBUG = "debug";

	public static final String PIG_JAVA_CLASS = "org.apache.pig.Main";

	public PigProcessJob(JobDescriptor descriptor) {
		super(descriptor);
	}

	@Override
	protected String getJavaClass() {
		return PIG_JAVA_CLASS;
	}

	@Override
	protected String getJVMArguments() {
		String args = super.getJVMArguments();

		List<String> udfImport = getUDFImportList();
		if (udfImport != null) {
			args += " -Dudf.import.list=" + super.createArguments(udfImport, ":");
		}

		String hadoopUGI = getHadoopUGI();
		if (hadoopUGI != null) {
			args += " -Dhadoop.job.ugi=" + hadoopUGI;
		}
		
		return args;
	}

	@Override
	protected String getMainArguments() {
		ArrayList<String> list = new ArrayList<String>();
		Map<String, String> map = getPigParams();
		if (map != null) {
			for (Map.Entry<String, String> entry : map.entrySet()) {
				list.add("-param " + StringUtils.shellQuote(entry.getKey() + "=" + entry.getValue(), StringUtils.SINGLE_QUOTE));
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

		return org.apache.commons.lang.StringUtils.join(list, " ");
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
		return getProps().getString(PIG_SCRIPT, getJobName() + ".pig");
	}

	protected List<String> getUDFImportList() {
		return getProps().getStringList(UDF_IMPORT, null, ",");
	}

	protected String getHadoopUGI() {
		return getProps().getString(HADOOP_UGI, null);
	}
	
	protected Map<String, String> getPigParams() {
		return getProps().getMapByPrefix(PIG_PARAM_PREFIX);
	}

	protected List<String> getPigParamFiles() {
		return getProps().getStringList(PIG_PARAM_FILES, null, ",");
	}
}
