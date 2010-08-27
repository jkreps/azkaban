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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import azkaban.common.utils.Props;

/**
 * A "composition" of flows.  This is composition in the functional sense.
 *
 * That is, the composition of two functions f(x) and g(x) is equivalent to f(g(x)).
 * Similarly, the composition of two ExecutableFlows A and B will result in a dependency
 * graph A -> B, meaning that B will be executed and complete before A.
 *
 * If B fails, A never runs.
 *
 * You should never really have to create one of these directly.  Try to use MultipleDependencyFlow
 * instead.
 */
public class ComposedFlow implements Flow
{
    private final Flow depender;
    private final Flow dependee;

    public ComposedFlow(Flow depender, Flow dependee)
    {
        this.depender = depender;
        this.dependee = dependee;
    }

    @Override
    public String getName()
    {
        return depender.getName();
    }

    @Override
    public boolean hasChildren()
    {
        return true;
    }

    @Override
    public List<Flow> getChildren()
    {
        return Arrays.asList(dependee);
    }

    @Override
    public ExecutableFlow createExecutableFlow(String id, Map<String, ExecutableFlow> overrides)
    {
        final ExecutableFlow dependeeFlow = overrides.containsKey(dependee.getName()) ?
                                            overrides.get(dependee.getName()) :
                                            dependee.createExecutableFlow(id, overrides);

        final ExecutableFlow dependerFlow = overrides.containsKey(depender.getName()) ?
                                            overrides.get(depender.getName()) :
                                            depender.createExecutableFlow(id, overrides);

        // Remove the depender from the overrides because the ComposedExecutableFlow will take its place.
        // The put() below will have the same effect, but I added this to be explicit.
        overrides.remove(depender.getName());

        final ComposedExecutableFlow retVal = new ComposedExecutableFlow(id, dependerFlow, dependeeFlow);

        if (overrides.containsKey(retVal.getName())) {
            throw new RuntimeException(String.format("overrides already has an entry with my key[%s], wtf?", retVal.getName()));
        }
        overrides.put(retVal.getName(), retVal);

        return retVal;
    }

    @Override
    public String toString()
    {
        return "ComposedExecutableFlow{" +
               "depender=" + depender +
               ", dependee=" + dependee +
               '}';
    }
}
