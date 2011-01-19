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
 * An ExecutableFlow is a Flow object that has both the graph structure data as well as state required to execute the
 * Flow.  While both an ExecutableFlow and a Flow are immutable from the API's perspective, an instance of an
 * ExecutableFlow represents a single execution of a Flow while an instance of a Flow simply represents the job
 * execution graph.
 */
public interface ExecutableFlow
{
    /**
     * Gets the id for the ExecutableFlow.  This is the same for all individual ExecutableFlow objects within this flow.
     *
     * @return the id for the flow
     */
    public String getId();

    /**
     * Gets the name of this stage of the flow.
     *
     * @return name of this stage of the flow.
     */
    public String getName();
    
    /**
     * Executes the flow.  This call should not block.  Instead, when execution is complete,
     * it should call the callback() method on the callback.
     *
     * Execute can be called multiple times and, upon completion, should call all callbacks that were passed into
     * an execute() call.  If the job is already completed when an execute() call is made, the callback should still
     * be called.
     *
     * @param parentProperties properties that should be used as parent properties by the execution.
     * @param callback the callback to be run upon completion.
     */
    public void execute(Props parentProperties, FlowCallback callback);

    /**
     * Cancels a running flow
     *
     * @return Whether the flow was canceled
     */
    public boolean cancel();

    /**
     * Gets the current state of the flow.
     *
     * @return the current state of the flow.
     */
    public Status getStatus();

    /**
     * Resets the state of the job such that it returns a Status of READY and whatever else that means.
     *
     * @return a boolean value indicating the success of the reset
     */
    public boolean reset();
    
    /**
     * Sets the state of a job to Status.COMPLETED.  This means that the job is considered "successful" for all intents
     * and purposes, but that it wasn't actually run.
     *
     * @return a boolean value indicating success.  A false return value generally means that adjusting the value at
     * that point could lead to a race condition or some other condition in which the internal state would not
     * be handled properly.
     */
    public boolean markCompleted();

    /**
     * Tells whether or not the Flow has children.  A Flow might have children if it encapsulates a dependency
     * relationship or if it is just grouping up multiple flows into a single unit.
     *
     * @return true if the flow has children
     */
    public boolean hasChildren();
    
    /**
     * Returns a list of the children Flow objects if any exist.  If there are no children, this should simply
     * return an empty list.
     *
     * @return A list of the children Flow objects.  An empty list if no children.  This should never return null.
     */
    public List<ExecutableFlow> getChildren();

    /**
     * Gets the time the execution started.  Null if not started yet.
     *
     * @return time execution started.  Null if not started yet.
     */
    public DateTime getStartTime();

    /**
     * Gets the time the execution completed.  Null if not started yet.
     *
     * @return time execution completed.  Null if not started yet.
     */
    public DateTime getEndTime();

    /**
     * Gets the parent props used for the execution of this Flow
     *
     * @return the parent props, null if state is READY
     */
    public Props getParentProps();

    /**
     * Gets the return props from the execution of this Flow
     *
     * The return props are the props that this execution wants to
     * provide to any other up-stream executions.  They are called
     * "return props" in following with the function metaphor, the
     * flow is essentially a function and it is "returning" this set
     * of properties to whatever told it to call.
     *
     * @return the parent props, null if state is READY
     */
    public Props getReturnProps();

    /**
     * Gets exceptions that caused this Flow to fail, if it has failed
     *
     * @return a map of job name to an exception if the Flow has failed
     */
    public Map<String, Throwable> getExceptions();
}
