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

package azkaban.jobs;

import static java.util.Arrays.asList;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.OptionException;

import org.apache.log4j.Logger;

import azkaban.app.PropsUtils;

import azkaban.common.utils.Props;

public class AzkabanCommandLine {

	private static Logger logger = Logger.getLogger(AzkabanCommandLine.class);

	/* option specs */
	private OptionSet options;
	private final OptionParser parser;
	private final String helpOpt = "help";
	private final OptionSpec<String> logDirOpt;
	private final OptionSpec<String> jobDirOpt;
	private final OptionSpec<String> configDirOpt;

	/* option values */
	private final File logDir;
	private final List<File> jobDirs;
	private final Props defaultProps;
	private final ClassLoader classloader;
	private final int numWorkPermits;

	public AzkabanCommandLine(String[] args) {
		this(new OptionParser(), args);
	}

	public AzkabanCommandLine(OptionParser parser, String[] args) {
		this.parser = parser;
		parser.acceptsAll(asList("h", helpOpt), "Print usage information");
		logDirOpt = parser.accepts("log-dir",
				"The directory to store log files.").withRequiredArg()
				.describedAs("dir");
		jobDirOpt = parser.acceptsAll(asList("j", "job-dir"),
				"A directory in which to find job definitions.")
				.withRequiredArg().describedAs("dir");
		configDirOpt = parser
				.acceptsAll(asList("c", "config-dir"),
						"A configuration directory for jobs, if seperate from the job directory.")
				.withRequiredArg().describedAs("dir");

		/* now parse options */
		try {
			this.options = parser.parse(args);
		} catch (OptionException e) {
			printHelpAndExit("Error parsing options: " + e.getMessage(),
					System.out);
		}

		if (options.has(logDirOpt))
			logDir = new File((String) options.valueOf(logDirOpt));
		else
			logDir = new File(System.getProperty("java.io.tmpdir"));

		jobDirs = new ArrayList<File>();
		for (String jobDir : options.valuesOf(jobDirOpt))
			jobDirs.add(new File(jobDir));

		if (options.has(configDirOpt)) {
			String confDir = options.valueOf(configDirOpt);
			logger.debug("Loading default properties from " + confDir);
			defaultProps = PropsUtils.loadPropsInDir(new File(confDir.trim()),
					".properties", ".schema");
		} else {
			logger.debug("No configuration options given (missing -c option).");
			defaultProps = new Props();
		}

		numWorkPermits = defaultProps.getInt("total.work.permits",
				Integer.MAX_VALUE);
		classloader = getHadoopClassLoader();
	}

	private ClassLoader getHadoopClassLoader() {
		String hadoopHome = System.getenv("HADOOP_HOME");
		if (hadoopHome != null) {
			try {
				logger.debug("Adding config to classloader from HADOOP_HOME ("
						+ hadoopHome + ").");
				return new URLClassLoader(new URL[] { new File(hadoopHome,
						"conf").toURI().toURL() }, CommandLineJobRunner.class
						.getClassLoader());
			} catch (MalformedURLException e) {
				throw new IllegalStateException("Invalid HADOOP_HOME: "
						+ hadoopHome + " is not a valid URL.");
			}
		} else {
			logger.debug("No HADOOP_HOME set, using default classloader.");
			return CommandLineJobRunner.class.getClassLoader();
		}
	}

	public boolean hasHelp() {
		return options.has(helpOpt);
	}

	public File getLogDir() {
		return logDir;
	}

	public List<File> getJobDirs() {
		return jobDirs;
	}

	public Props getDefaultProps() {
		return defaultProps;
	}

	public ClassLoader getClassloader() {
		return classloader;
	}

	public int getNumWorkPermits() {
		return numWorkPermits;
	}

	public OptionSet getOptions() {
		return this.options;
	}

	public OptionParser getParser() {
		return this.parser;
	}

	public void printHelpAndExit(String message, PrintStream out) {
		out.println(message);
		try {
			parser.printHelpOn(out);
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.exit(1);
	}

}
