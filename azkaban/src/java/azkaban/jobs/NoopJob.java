package azkaban.jobs;

import azkaban.app.JobDescriptor;
import azkaban.common.jobs.Job;
import azkaban.common.utils.Props;

/**
 *
 */
public class NoopJob implements Job
{
    public NoopJob(JobDescriptor descriptor)
    {
        
    }

    @Override
    public String getId()
    {
        return "Azkaban!! -- " + getClass().getName();
    }

    @Override
    public void run() throws Exception
    {
    }

    @Override
    public void cancel() throws Exception
    {
    }

    @Override
    public double getProgress() throws Exception
    {
        return 0;
    }

    @Override
    public Props getJobGeneratedProperties()
    {
        return new Props();
    }
}
