package azkaban.flow;

import azkaban.common.utils.Props;

import java.util.*;

/**
 * A flow that pushes properties down into its subflows.
 *
 * This acts as a method of allowing for parameterized flows.  You
 * can create a workflow that is set up to do various tasks and have
 * it paramaterized by some properties.
 *
 * Just set up your workflow such that this job is in front of the 
 * parameterized flow and provide the Props that you want passed down
 */
public class PropertyPushingFlow implements Flow
{
    private final String name;
    private final Props props;
    private final Flow[] children;

    public PropertyPushingFlow(String name, Props props, Flow... children)
    {
        this.name = name;
        this.props = props;
        this.children = children;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean hasChildren() {
        return true;
    }

    @Override
    public List<Flow> getChildren() {
        return Arrays.asList(children);
    }

    @Override
    public ExecutableFlow createExecutableFlow(String id, Map<String, ExecutableFlow> overrides) {
        ExecutableFlow[] executableFlows = new ExecutableFlow[children.length];

        Map<String, ExecutableFlow> overridesForSubFlow = new HashMap<String, ExecutableFlow>();
        for (int i = 0; i < children.length; ++i) {
            final Flow child = children[i];

            if (overridesForSubFlow.containsKey(child.getName())) {
                executableFlows[i] = overridesForSubFlow.get(child.getName());
            }
            else {
                executableFlows[i] = child.createExecutableFlow(id, overridesForSubFlow);
            }
        }

        final PropertyPushingExecutableFlow retVal = new PropertyPushingExecutableFlow(id, name, props, executableFlows);

        overrides.put(retVal.getId(), retVal);

        return retVal;
    }
}
