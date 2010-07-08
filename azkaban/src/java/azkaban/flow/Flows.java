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

import java.util.List;
import java.util.Map;
import java.util.Set;

import azkaban.app.JobDescriptor;
import azkaban.app.JobManager;
import azkaban.app.JobWrappingFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 *
 */
public class Flows
{
    public static Flow buildLegacyFlow(
            final JobWrappingFactory jobFactory,
            final JobManager jobManager,
            final Map<String, Flow> alreadyBuiltFlows,
            final JobDescriptor rootDescriptor
    )
    {
        //TODO MED: The jobManager isn't really the best Job factory.  It should be revisited, but it works for now.
        if (alreadyBuiltFlows.containsKey(rootDescriptor.getId())) {
            return alreadyBuiltFlows.get(rootDescriptor.getId());
        }

        final Flow retVal;
        if (rootDescriptor.hasDependencies()) {
            Set<JobDescriptor> dependencies = rootDescriptor.getDependencies();
            List<Flow> dependencyFlows =
                    Lists.newArrayList(
                            Iterables.transform(
                                    dependencies,
                                    new Function<JobDescriptor, Flow>()
                                    {
                                        @Override
                                        public Flow apply(JobDescriptor jobDescriptor)
                                        {
                                            return buildLegacyFlow(jobFactory, jobManager, alreadyBuiltFlows, jobDescriptor);
                                        }
                                    }
                            )
                    );

            retVal = new MultipleDependencyFlow(
                    new IndividualJobFlow(
                            rootDescriptor.getId(),
                            jobManager
                            ),
                    dependencyFlows.toArray(new Flow[dependencyFlows.size()])
            );
        }
        else {
            retVal = new IndividualJobFlow(
                    rootDescriptor.getId(),
                    jobManager
            );
        }

        alreadyBuiltFlows.put(retVal.getName(), retVal);

        return retVal;
    }

    public static ExecutableFlow resetFailedFlows(
            final ExecutableFlow theFlow
    )
    {
        if (theFlow.getStatus() == Status.FAILED) {
            theFlow.reset();
        }

        if (theFlow.hasChildren()) {
            for (ExecutableFlow flow : theFlow.getChildren()) {
                resetFailedFlows(flow);
            }
        }

        return theFlow;
    }

}
