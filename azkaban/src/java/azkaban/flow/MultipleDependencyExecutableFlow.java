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

import azkaban.common.utils.Props;
import azkaban.jobs.Status;

import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;

/**
 * A flow that provides an API for creating a dependency graph.
 *
 * The class takes a minimum of two ExecutableFlow objects on the constructor and builds
 * a dependency relationship between them.  The first ExecutableFlow object will not run
 * until all of the other flows (second, third, ...) have succeeded.  If any of the
 * dependee flows fails, the depender flow will not run.
 *
 * That is, if you have flows A, B and C, and you want A to run only after B and C have
 * completed, simply constructor a
 *
 * new MultipleDependencyExecutableFlow("some_id", A, B, C);
 *
 * This class makes use of ComposedExecutableFlow and GroupedExecutableFlow under the covers
 * to ensure this behavior, but it exposes a more stream-lined "view" of the dependency graph that
 * makes it easier to reason about traversals of the resultant DAG.
 */
public class MultipleDependencyExecutableFlow implements ExecutableFlow
{
    private final GroupedExecutableFlow dependeesGrouping;
    private final ComposedExecutableFlow actualFlow;
    private final String id;
    private final ExecutableFlow depender;

    public MultipleDependencyExecutableFlow(String id, ExecutableFlow depender, ExecutableFlow... dependees)
    {
        this.id = id;
        this.depender = depender;
        dependeesGrouping = new GroupedExecutableFlow(id, dependees);
        actualFlow = new ComposedExecutableFlow(id, this.depender, dependeesGrouping);
    }

    @Override
    public String getId()
    {
        return id;
    }

    @Override
    public String getName()
    {
        return actualFlow.getName();
    }

    @Override
    public void execute(Props parentProperties, FlowCallback callback)
    {
        actualFlow.execute(parentProperties, callback);
    }

    @Override
    public boolean cancel()
    {
        return actualFlow.cancel();
    }

    @Override
    public Status getStatus()
    {
        return actualFlow.getStatus();
    }

    @Override
    public boolean reset()
    {
        final boolean actualFlowReset = actualFlow.reset();

        for (ExecutableFlow flow : actualFlow.getChildren()) {
            flow.reset();
        }

        return actualFlowReset;
    }

    @Override
    public boolean markCompleted()
    {
        return actualFlow.markCompleted();
    }

    @Override
    public boolean hasChildren()
    {
        return true;
    }

    @Override
    public List<ExecutableFlow> getChildren()
    {
        return dependeesGrouping.getChildren();
    }

    @Override
    public DateTime getStartTime()
    {
        return actualFlow.getStartTime();
    }

    @Override
    public DateTime getEndTime()
    {
        return actualFlow.getEndTime();
    }

    @Override
    public Props getParentProps()
    {
        return actualFlow.getParentProps();
    }

    @Override
    public Props getReturnProps() {
        return actualFlow.getReturnProps();
    }

    @Override
    public Map<String,Throwable> getExceptions()
    {
        return actualFlow.getExceptions();
    }

    @Override
    public String toString()
    {
        return "MultipleDependencyExecutableFlow{" +
               "dependeesGrouping=" + dependeesGrouping +
               ", actualFlow=" + actualFlow +
               '}';
    }

    public ComposedExecutableFlow getActualFlow()
    {
        return actualFlow;
    }
}
