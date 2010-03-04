package azkaban.flow;

import java.util.List;
import java.util.Map;

/**
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
    public ExecutableFlow createExecutableFlow(String id, Map<String, ExecutableFlow> overrides);
}
