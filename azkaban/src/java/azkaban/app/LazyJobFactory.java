package azkaban.app;

import azkaban.common.jobs.Job;

/**
*
*/
public class LazyJobFactory implements JobFactory
{
    private final JobWrappingFactory jobFactory;
    private final JobManager jobManager;
    private final String name;

    public LazyJobFactory(JobWrappingFactory jobFactory, JobManager jobManager, String name)
    {
        this.jobFactory = jobFactory;
        this.jobManager = jobManager;
        this.name = name;
    }

    @Override
    public Job factorizeJob()
    {
        return jobFactory.apply(jobManager.getJobDescriptor(name));
    }
}
