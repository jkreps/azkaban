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

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import azkaban.common.utils.Props;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

/**
 * A grouping of flows.
 *
 * For example, if you had two functions f(x) and g(x) that you wanted to execute together, you
 * could conceivably create a new function h(x) = { f(x); g(x); }.  That is essentially what
 * this class does with ExecutableFlow objects.
 *
 * You should never really have to create one of these directly.  Try to use MultipleDependencyFlow
 * instead.
 */
public class GroupedFlow implements Flow
{
    private final Flow[] flows;
    private final Flow[] sortedFlows;

    public GroupedFlow(Flow... flows)
    {
        this.flows = flows;
        this.sortedFlows = Arrays.copyOf(this.flows, this.flows.length);
        Arrays.sort(this.sortedFlows, new Comparator<Flow>()
        {
            @Override
            public int compare(Flow o1, Flow o2)
            {
                return o1.getName().compareTo(o2.getName());
            }
        });
    }

    @Override
    public String getName()
    {
        return StringUtils.join(
                Iterables.transform(Arrays.asList(flows), new Function<Flow, String>()
                {
                    @Override
                    public String apply(Flow flow)
                    {
                        return flow.getName();
                    }
                }).iterator(),
                " + "
        );
    }

    @Override
    public boolean hasChildren()
    {
        return true;
    }

    @Override
    public List<Flow> getChildren()
    {
        return Arrays.asList(sortedFlows);
    }

    @Override
    public ExecutableFlow createExecutableFlow(String id, Map<String, ExecutableFlow> overrides)
    {
        ExecutableFlow executableFlows[] = new ExecutableFlow[flows.length];

        for (int i = 0; i < executableFlows.length; i++) {
            executableFlows[i] = overrides.containsKey(flows[i].getName()) ?
                                 overrides.get(flows[i].getName()) :
                                 flows[i].createExecutableFlow(id, overrides);
        }

        // Grouped(Executable)Flow is just an abstraction, it doesn't represent anything concrete and thus shouldn't
        // be put in the overrides map.

        return new GroupedExecutableFlow(id, executableFlows);
    }

    @Override
    public String toString()
    {
        return "GroupedExecutableFlow{" +
               "flows=" + (flows == null ? null : Arrays.asList(flows)) +
               '}';
    }
}