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

package azkaban.serialization.de;

import azkaban.flow.*;
import azkaban.serialization.MultipleDependencyEFSerializer;
import azkaban.serialization.Verifier;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.*;

/**
 *
 */
public class ExecutableFlowDeserializer implements Function<Map<String, Object>, ExecutableFlow>
{
    private volatile Function<Map<String, Object>, ExecutableFlow> jobDeserializer;

    public void setJobDeserializer(Function<Map<String, Object>, ExecutableFlow> jobDeserializer) {
        this.jobDeserializer = jobDeserializer;
    }

    @Override
    public ExecutableFlow apply(Map<String, Object> descriptor)
    {
        Map<String, ExecutableFlow> jobs = Maps.uniqueIndex(
                Iterables.<Map<String, Object>, ExecutableFlow>transform(
                        Verifier.getVerifiedObject(descriptor, "jobs", Map.class).values(),
                        jobDeserializer
                        ),
                new Function<ExecutableFlow, String>()
                {
                    @Override
                    public String apply(ExecutableFlow flow)
                    {
                        return flow.getName();
                    }
                }
        );

        Map<String, List<String>> dependencies = Verifier.getVerifiedObject(descriptor, "dependencies", Map.class);
        List<String> roots = Verifier.getVerifiedObject(descriptor, "root", List.class);
        String id = Verifier.getString(descriptor, "id");
        
        return buildFlow(id, roots, dependencies, jobs);
    }

    private ExecutableFlow buildFlow(
            final String id,
            Iterable<String> roots,
            final Map<String, List<String>> dependencies,
            final Map<String, ExecutableFlow> jobs
    )
    {
        final ArrayList<ExecutableFlow> executableFlows = Lists.newArrayList(
                Iterables.transform(
                        roots,
                        new Function<String, ExecutableFlow>()
                        {
                            @Override
                            public ExecutableFlow apply(String root)
                            {
                                if (dependencies.containsKey(root)) {
                                    final ExecutableFlow dependeeFlow = buildFlow(id, dependencies.get(root), dependencies, jobs);

                                    if (dependeeFlow instanceof GroupedExecutableFlow) {
                                        return new MultipleDependencyExecutableFlow(
                                                id,
                                                buildFlow(id, Arrays.asList(root), Collections.<String, List<String>>emptyMap(), jobs),
                                                (ExecutableFlow[]) dependeeFlow.getChildren().toArray()
                                        );
                                    }
                                    else {
                                        return new ComposedExecutableFlow(
                                                id,
                                                buildFlow(id, Arrays.asList(root), Collections.<String, List<String>>emptyMap(), jobs),
                                                dependeeFlow
                                        );
                                    }

                                }
                                else {
                                    if (! jobs.containsKey(root)) {
                                        throw new IllegalStateException(String.format(
                                                "Expected job[%s] in jobs list[%s]",
                                                root,
                                                jobs
                                        ));
                                    }

                                    return jobs.get(root);
                                }
                            }
                        }
                )
        );

        if (executableFlows.size() == 1) {
            return executableFlows.get(0);
        }
        else {
            return new GroupedExecutableFlow(id, executableFlows.toArray(new ExecutableFlow[executableFlows.size()]));
        }
    }
}
