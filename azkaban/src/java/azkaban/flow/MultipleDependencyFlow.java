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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import azkaban.common.utils.Props;

/**
 * A flow that provides an API for creating a dependency graph.
 *
 * The class takes a minimum of two Flow objects on the constructor and builds
 * a dependency relationship between them.  The first Flow object dependant on the
 * others (second, third, ...).
 *
 * That is, if you have flows A, B and C, and you want A to depend on the execution of
 * B and C, simply constructor a
 *
 * new MultipleDependencyFlow(A, B, C);
 *
 * This class makes use of ComposedFlow and GroupedFlow under the covers
 * to ensure this behavior, but it exposes a more stream-lined "view" of the dependency
 * graph that makes it easier to reason about traversals of the resultant DAG.
 */
public class MultipleDependencyFlow implements Flow
{
    private final GroupedFlow dependeesGrouping;
    private final ComposedFlow actualFlow;
    private final Flow depender;

    public MultipleDependencyFlow(Flow depender, Flow... dependees)
    {
        this.depender = depender;
        dependeesGrouping = new GroupedFlow(dependees);
        actualFlow = new ComposedFlow(this.depender, dependeesGrouping);
    }

    @Override
    public String getName()
    {
        return actualFlow.getName();
    }

    @Override
    public boolean hasChildren()
    {
        return true;
    }

    @Override
    public List<Flow> getChildren()
    {
        return dependeesGrouping.getChildren();
    }

    @Override
    public ExecutableFlow createExecutableFlow(String id, Map<String, ExecutableFlow> overrides)
    {
        final List<ExecutableFlow> childList = dependeesGrouping.createExecutableFlow(id, overrides).getChildren();
        ExecutableFlow[] executableChildren = childList.toArray(new ExecutableFlow[childList.size()]);

        final ExecutableFlow dependerFlow = overrides.containsKey(depender.getName()) ?
                                            overrides.get(depender.getName()) :
                                            depender.createExecutableFlow(id, new HashMap<String, ExecutableFlow>());


        final MultipleDependencyExecutableFlow retVal = new MultipleDependencyExecutableFlow(id, dependerFlow, executableChildren);

        if (overrides.containsKey(retVal.getName())) {
            throw new RuntimeException(String.format("overrides already has an entry with my key[%s], wtf?", retVal.getName()));
        }
        overrides.put(retVal.getName(), retVal);

        return retVal;
    }

    @Override
    public String toString()
    {
        return "MultipleDependencyExecutableFlow{" +
               "dependeesGrouping=" + dependeesGrouping +
               ", actualFlow=" + actualFlow +
               '}';
    }
}