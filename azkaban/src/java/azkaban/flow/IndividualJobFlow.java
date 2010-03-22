package azkaban.flow;

import azkaban.app.JobDescriptor;
import azkaban.app.JobFactory;
import azkaban.app.JobWrappingFactory;
import azkaban.common.jobs.Job;

import java.util.Collections;
import java.util.List;
import java.util.Map;


/**
 *
 */
public class IndividualJobFlow implements Flow
{
    private final JobFactory jobFactory;
    private final String name;

    public IndividualJobFlow(String name, JobFactory jobFactory)
    {
        this.name = name;
        this.jobFactory = jobFactory;
    }

    @Override
    public String getName()
    {
        return name;
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
                                      new IndividualJobExecutableFlow(id, name, jobFactory);

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
               "job=" + name +
               '}';
    }
}