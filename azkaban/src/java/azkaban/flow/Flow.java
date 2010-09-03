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

import azkaban.common.utils.Props;

/**
 * An immutable class that represents a given workflow.
 *
 * This class primarily exists to allow for a separation between the definition
 * of a workflow and the execution of that same workflow.  A Flow object can be
 * seen as a factory of "workflow execution" objects, represented by the class
 * ExecutableFlow.
 *
 * In order to support restart, however, Flows should not be used as factories directly
 * but should instead be used through a FlowManager instance that will do the requisite
 * id bookkeeping to allow for restarts. 
 */
public interface Flow {

    /**
     * Gets the name of this stage of the flow.
     *
     * @return name of this stage of the flow.
     */
    public String getName();

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
    public List<Flow> getChildren();

    /**
     * Builds an ExecutableFlow from the current flow object.
     *
     * @argument overrides a map of overrides that should be used in the case that any of the names of the
     * child nodes already exists.  This map is intended to be modified as a side-effect of these calls and will
     * be a map of all nodes in the flow after execution is complete.
     *
     * @return an ExecutableFlow that, when executed, will run the flow represented by this object
     */
    ExecutableFlow createExecutableFlow(String id, Map<String, ExecutableFlow> overrides);
}
