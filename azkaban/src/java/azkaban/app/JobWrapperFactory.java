package azkaban.app;

import java.util.HashMap;

import azkaban.common.jobs.Job;
import azkaban.common.utils.Utils;

public class JobWrapperFactory {

	private HashMap<String, Class<? extends Job>> _jobToClass = new HashMap<String, Class<? extends Job>>();
	private String _defaultType = "java";
	
	public JobWrapperFactory() {
	}
	
	public void setDefaultType(String type) {
		_defaultType = type;
	}
	
	public String getDefaultType() {
		return _defaultType;
	}
	
	public void registerJobExecutorType(String type, Class<? extends Job> jobClass) {
		_jobToClass.put(type, jobClass);
	}
	
	public Job getJobExecutor(JobDescriptor descriptor) {
		Job executor = null;
		
		String jobType = descriptor.getJobType();
		if (jobType == null || jobType.length() == 0) {
			jobType = _defaultType;
		}
		Class<? extends Job> executorClass = _jobToClass.get(jobType);
		
		if (executorClass == null) {
			throw new JobExecutionException(
                    String.format(
                            "Could not construct job[%s] of type[%s].",
                            descriptor,
                            jobType
                    ));
		}
		else {
			executor = (Job)Utils.callConstructor(executorClass, descriptor);
		}
		
		return executor;
	}
}