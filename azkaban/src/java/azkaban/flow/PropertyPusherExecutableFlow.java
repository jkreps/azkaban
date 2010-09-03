package azkaban.flow;

import azkaban.common.utils.Props;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A flow that pushes properties down into its subflows.
 *
 * This acts as a method of allowing for parameterized flows.  You
 * can create a workflow that is set up to do various tasks and have
 * it paramaterized by some properties.
 *
 * Just set up your workflow such that this job is in front of (depends on)
 * the parameterized flow and provide the Props that you want passed down
 */
public class PropertyPusherExecutableFlow extends WrappingExecutableFlow
{
    private final String id;
    private final String name;
    private final List<ExecutableFlow> allChildren;

    public PropertyPusherExecutableFlow(
            String id,
            String name,
            ExecutableFlow propertyFlow,
            ExecutableFlow... children
    )
    {
        super(new ComposedExecutableFlow(id, new GroupedExecutableFlow(id, children), propertyFlow));
        this.id = id;
        this.name = name;

        this.allChildren = new ArrayList<ExecutableFlow>(children.length + 1);
        allChildren.add(propertyFlow);
        allChildren.addAll(((ComposedExecutableFlow) getDelegateFlow()).getDepender().getChildren());
    }

    @Override
    public String getId()
    {
        return id;
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public List<ExecutableFlow> getChildren()
    {
        return allChildren;
    }

    public ExecutableFlow getPropertyGeneratingFlow()
    {
        return ((ComposedExecutableFlow) getDelegateFlow()).getDependee();
    }

    public ExecutableFlow getSubFlow()
    {
        return ((ComposedExecutableFlow) getDelegateFlow()).getDepender();
    }

}
