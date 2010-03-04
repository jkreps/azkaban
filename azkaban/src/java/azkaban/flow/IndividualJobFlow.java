package azkaban.flow;

import azkaban.app.JavaJob;
import azkaban.common.jobs.Job;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class IndividualJobFlow implements Flow
{
    private final Job job;

    public IndividualJobFlow(Job job)
    {
        this.job = job;
    }

    @Override
    public String getName()
    {
        return job.getId();
    }

    @Override
    public boolean hasChildren()
    {
        return false;
    }

    @Override
    public List<Flow> getChildren()
    {
        return Collections.emptyList();
    }

    @Override
    public ExecutableFlow createExecutableFlow(String id, Map<String, ExecutableFlow> overrides)
    {
        final ExecutableFlow retVal = overrides.containsKey(getName()) ?
                                      overrides.get(getName()) :
                                      new IndividualJobExecutableFlow(id, job);

        if (overrides.containsKey(retVal.getName())) {
            throw new RuntimeException(String.format("overrides already has an entry with my key[%s], wtf?", retVal.getName()));
        }
        overrides.put(retVal.getName(), retVal);

        return retVal;
    }

    @Override
    public String toString()
    {
        return "IndividualJobExecutableFlow{" +
               "job=" + job +
               '}';
    }
}