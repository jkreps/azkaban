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