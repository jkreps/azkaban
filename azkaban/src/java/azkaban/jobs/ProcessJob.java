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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Level;

import azkaban.app.JobDescriptor;
import azkaban.common.jobs.AbstractJob;
import azkaban.common.jobs.Job;
import azkaban.common.utils.Props;

/**
 * A job that runs a simple unix command
 * 
 * @author jkreps
 * 
 */
public class ProcessJob extends AbstractJob implements Job {

	public static final String ENV_PREFIX = "env.";
	public static final String COMMAND = "command";
	public static final String WORKING_DIR = "working.dir";

	private final Props _props;
	private final String _jobPath;
	private final String _name;
	private volatile Process _process;
	private volatile boolean _isComplete;

	public ProcessJob(JobDescriptor descriptor) {
		super(descriptor.getId());
		this._props = descriptor.getProps();
		this._isComplete = false;
		this._jobPath = descriptor.getFullPath();
		this._name = descriptor.getId();
	}

	public void run() {
		// Sets a list of all the commands that need to be run.
		List<String> commands = getCommandList();
		info(commands.size() + " commands to execute.");

		Map<String, String> env = getEnvironmentVariables();

		String cwd = getWorkingDirectory();

		// For each of the jobs, set up a process and run them.
		for (String command : commands) {
			info("Executing command: " + command);
			String[] cmdPieces = command.split("\\s+");
			ProcessBuilder builder = new ProcessBuilder(cmdPieces);

			builder.directory(new File(cwd));
			builder.environment().putAll(env);

			try {
				_process = builder.start();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			Thread outputGobbler = new LoggingGobbler(new InputStreamReader(
					_process.getInputStream()), Level.INFO);
			Thread errorGobbler = new LoggingGobbler(new InputStreamReader(
					_process.getErrorStream()), Level.ERROR);
			outputGobbler.start();
			errorGobbler.start();
			int exitCode = 0;
			try {
				exitCode = _process.waitFor();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}

			_isComplete = true;
			if (exitCode != 0)
				throw new RuntimeException("Processes ended with exit code "
						+ exitCode + ".");

			// try to wait for everything to get logged out before exiting
			try {
				outputGobbler.join(1000);
				errorGobbler.join(1000);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}

	protected List<String> getCommandList() {
		List<String> commands = new ArrayList<String>();
		commands.add(_props.getString(COMMAND));
		for (int i = 1; _props.containsKey(COMMAND + "." + i); i++)
			commands.add(_props.getString(COMMAND + "." + i));

		return commands;
	}

	protected Map<String, String> getEnvironmentVariables() {
		return getMapFromPrefixProperties(ENV_PREFIX);
	}

	protected String getWorkingDirectory() {
		return _props.containsKey(WORKING_DIR) ? _props.getString(WORKING_DIR)
				: new File(_jobPath).getParent();
	}

	protected final Map<String, String> getMapFromPrefixProperties(String prefix) {
		Map<String, String> prefixProperties = new HashMap<String, String>();
		Set<String> keys = _props.keySet();
		for (String key : keys) {
			if (key.toLowerCase().startsWith(prefix)) {
				String value = _props.getString(key);
				String strippedKey = key.substring(prefix.length());
				prefixProperties.put(strippedKey, value);
			}
		}

		return prefixProperties;
	}

	@Override
	public void cancel() throws Exception {
		if (_process != null)
			_process.destroy();
	}

	@Override
	public double getProgress() {
		return _isComplete ? 1.0 : 0.0;
	}

	private class LoggingGobbler extends Thread {

		private final BufferedReader _inputReader;
		private final Level _loggingLevel;

		public LoggingGobbler(InputStreamReader inputReader, Level level) {
			_inputReader = new BufferedReader(inputReader);
			_loggingLevel = level;
		}

		@Override
		public void run() {
			try {
				while (!Thread.currentThread().isInterrupted()) {
					String line = _inputReader.readLine();
					if (line == null)
						return;
					getLog().log(_loggingLevel, line);
				}
			} catch (IOException e) {
				error("Error reading from logging stream:", e);
			}
		}
	}

	public Props getProps() {
		return _props;
	}

	public String getPath() {
		return _jobPath;
	}
	
	public String getJobName() {
		return _name;
	}
}
