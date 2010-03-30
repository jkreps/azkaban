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

import java.lang.reflect.Method;
import java.util.Properties;

import azkaban.app.JobDescriptor;
import azkaban.common.jobs.AbstractJob;
import azkaban.common.jobs.Job;
import azkaban.common.utils.Utils;

public class JavaJob extends AbstractJob implements Job {

	public static final String RUN_METHOD_PARAM = "method.run";
	public static final String CANCEL_METHOD_PARAM = "method.cancel";
	public static final String PROGRESS_METHOD_PARAM = "method.progress";

	public static final String JOB_CLASS = "job.class";
	public static final String DEFAULT_CANCEL_METHOD = "cancel";
	public static final String DEFAULT_RUN_METHOD = "run";
	public static final String DEFAULT_PROGRESS_METHOD = "getProgress";

	private String _runMethod;
	private String _cancelMethod;
	private String _progressMethod;

	private Object _javaObject = null;
	private JobDescriptor _descriptor;

	public JavaJob(JobDescriptor descriptor) {
		super(descriptor.getId());

		if (descriptor.getJobClass() == null) {
			throw new IllegalArgumentException(
					String
							.format(
									"A java job descriptor with no class is fairly pointless. JobDescriptor[%s]",
									descriptor));
		}

		_descriptor = descriptor;
		_runMethod = _descriptor.getProps().getString(RUN_METHOD_PARAM,
				DEFAULT_RUN_METHOD);
		_cancelMethod = _descriptor.getProps().getString(CANCEL_METHOD_PARAM,
				DEFAULT_CANCEL_METHOD);
		_progressMethod = _descriptor.getProps().getString(
				PROGRESS_METHOD_PARAM, DEFAULT_PROGRESS_METHOD);

	}

	@Override
	public void cancel() throws Exception {
		// look for cancel method
		if (_javaObject != null) {

			Method method = Utils.getMethod(_javaObject.getClass(),
					_cancelMethod, new Class<?>[]{});

			if (method != null)
				method.invoke(_javaObject, (Object)null);
			else {
				throw new RuntimeException("Job " + getId()
						+ " does not support cancellation!");
			}
		}
	}

	@Override
	public String getId() {
		return _descriptor.getId();
	}

	@Override
	public double getProgress() throws Exception {
		if (_javaObject != null) {

			Method method = Utils.getMethod(_javaObject.getClass(),
					_progressMethod, new Class<?>[]{});

			if (method != null) {
				Object progress = method.invoke(_javaObject, new Object[]{});

				if (progress instanceof Double) {
					return (Double) progress;
				}
			}
		}
		return super.getProgress();
	}


	public void run() {
		@SuppressWarnings("unused")
		ClassLoader loader = getClass().getClassLoader();

		if (Utils.constructorExist(_descriptor.getJobClass(), getId(),
				_descriptor.getProps())) {
			_javaObject = Utils.callConstructor(_descriptor.getJobClass(),
					getId(), _descriptor.getProps());
		} else if (Utils.constructorExist(_descriptor.getJobClass(),
				_descriptor.getProps())) {
			_javaObject = Utils.callConstructor(_descriptor.getJobClass(),
					_descriptor.getProps());
		} else if (Utils.constructorExist(_descriptor.getJobClass(),
				new Properties())) {
			Properties properties = _descriptor.getProps().toProperties();
			_javaObject = Utils.callConstructor(_descriptor.getJobClass(),
					properties);
		} else if (Utils.constructorExist(_descriptor.getJobClass(), getId())) {
			_javaObject = Utils.callConstructor(_descriptor.getJobClass(),
					getId());
		} else {
			_javaObject = Utils.callConstructor(_descriptor.getJobClass());
		}

		try {
			Utils.getMethod(_javaObject.getClass(), _runMethod, new Class<?>[]{}).invoke(
					_javaObject, new Object[]{});
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public JobDescriptor getDescriptor() {
		return _descriptor;
	}

	@Override
	public String toString() {
		return "JavaJob{" + "_runMethod='" + _runMethod + '\''
				+ ", _cancelMethod='" + _cancelMethod + '\''
				+ ", _progressMethod='" + _progressMethod + '\''
				+ ", _javaObject=" + _javaObject + ", _descriptor="
				+ _descriptor + '}';
	}
}
