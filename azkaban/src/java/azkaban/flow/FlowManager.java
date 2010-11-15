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

import azkaban.flow.ExecutableFlow;
import azkaban.flow.Flow;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import azkaban.common.utils.Props;

/**
 *
 */
public interface FlowManager extends Iterable<Flow>
{
    /**
     * Checks if a flow exists with the given name.
     *
     * @param name name of flow
     * @return if the flow exists
     */
    boolean hasFlow(String name);

    /**
     * Gets the Flow object for the flow with the given name.
     *
     * @param name name of flow
     * @return the flow with the given name
     */
    Flow getFlow(String name);

    /**
     * Returns all the base folders
     * @return
     */
    List<String> getFolders();
    
    /**
     * Returns all the root names by folder
     * 
     * @param folder
     * @return
     */
    List<String> getRootNamesByFolder(String folder);
    
    /**
     * Gets all known Flow objects
     *
     * @return a Collection of all known Flow objects
     */
    Collection<Flow> getFlows();

    /**
     * Gets all root Flow objects.  A "root" flow is defined as a flow with no parent
     *
     * @return a Set of all root Flow objects
     */
    Set<String> getRootFlowNames();
    
    /**
     * An Iterator over the set of all known Flows.  This is the same as calling
     * getFlows().iterator();
     *
     * @return An Iterator over the set of all known flows.
     */
    @Override
    Iterator<Flow> iterator();

    /**
     * Creates an ExecutableFlow for the Flow with the given name.
     *
     * @param name name of flow
     * @return A clone of the Flow that is executable
     */
    ExecutableFlow createNewExecutableFlow(String name);

    /**
     * A method that will return a unique "next id".  The contract around this method is that if value X will only ever
     * be returned once from getNextId().
     *
     * This method probably doesn't belong in this API, but it is required right now to support reloading.  Implementing
     * functionality external from a FlowManager using this method is highly discouraged.
     *
     * @return the next Id for a Flow
     */
    long getNextId();

    /**
     * Gets the current maxId.
     *
     * This method probably doesn't belong in this API, but it is required right now to support reloading.  Implementing
     * functionality external from a FlowManager using this method is highly discouraged.
     *
     * @return the current max Id
     */
    long getCurrMaxId();

    /**
     * Saves an ExecutableFlow.  The index for saving ExecutableFlows is the Id of the flow.  Thus, if the
     * flow passed in to this method shares an id with a previously saved flow, the previous save will be overwritten.
     *
     * @param flow the ExecutableFlow to be saved
     * @return the ExecutableFlow that was saved (the argument passed in to this method).
     */
    FlowExecutionHolder saveExecutableFlow(FlowExecutionHolder flow);

    /**
     * Loads an ExecutableFlow.  The index for loading the ExecutableFlow is its id.  If a flow with id 7 was previously
     * saved, then calling this method and passing in a 7 will load up that previous flow with all of its state.
     *
     * The dependence on the long id as the index for a Flow is an arbitrary decision that might be reevaluated.
     *
     * @param id id of the flow to load
     * @return the flow with said id, null if doesn't exist
     */
    FlowExecutionHolder loadExecutableFlow(long id);

    /**
     * Tells the FlowManager to reload its flows.
     */
    void reload();
}
