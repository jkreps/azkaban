package azkaban.flow;

import azkaban.common.utils.Props;

import java.util.*;

/**
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

            if (overrides.containsKey(child.getName())) {
                executableFlows[i] = overrides.get(child.getName());
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
