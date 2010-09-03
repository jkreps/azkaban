package azkaban.flow;

import azkaban.common.utils.Props;
import com.google.common.collect.Sets;

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
public class PropertyPusherFlow implements Flow
{
    private static final Set<String> blacklistedKeySet = Sets.newHashSet(
            "type",
            "dependencies",
            "prop-dependency"
    );

    private final String name;
    private final Flow propertyFlow;
    private final Props props;
    private final Flow[] children;
    private final List<Flow> allChildren;

    public PropertyPusherFlow(String name, Flow propertyFlow, Flow... children)
    {
        this.name = name;
        this.propertyFlow = propertyFlow;
        this.props = new Props();
        this.children = children;

        for (String key : props.getKeySet()) {
            if (blacklistedKeySet.contains(key)) {
                continue;
            }
            this.props.put(key, props.get(key));
        }

        Flow[] sortedChildren = Arrays.copyOf(children, children.length);
        Arrays.sort(sortedChildren, new Comparator<Flow>()
        {
            @Override
            public int compare(Flow o1, Flow o2)
            {
                return o1.getName().compareTo(o2.getName());
            }
        });

        allChildren = new ArrayList<Flow>();
        allChildren.add(propertyFlow);
        allChildren.addAll(Arrays.asList(sortedChildren));
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
        return allChildren;
    }

    @Override
    public ExecutableFlow createExecutableFlow(String id, Map<String, ExecutableFlow> overrides) {
        ExecutableFlow[] executableFlows = new ExecutableFlow[children.length];

        ExecutableFlow propertyExecutableFlow = propertyFlow.createExecutableFlow(id, overrides);

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

        final PropertyPusherExecutableFlow retVal = new PropertyPusherExecutableFlow(id, name, propertyExecutableFlow, executableFlows);

        overrides.put(retVal.getId(), retVal);

        return retVal;
    }
}
