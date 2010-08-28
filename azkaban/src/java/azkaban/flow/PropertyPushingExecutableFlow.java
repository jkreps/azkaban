package azkaban.flow;

import azkaban.common.utils.Props;
import org.joda.time.DateTime;

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
public class PropertyPushingExecutableFlow extends WrappingExecutableFlow
{
    private final String id;
    private final String name;
    private final Props props;

    public PropertyPushingExecutableFlow(
            String id,
            String name,
            Props props,
            ExecutableFlow... children
    )
    {
        super(new GroupedExecutableFlow(name + "-subflow", children));
        this.id = id;
        this.name = name;
        this.props = props;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void execute(Props parentProperties, FlowCallback callback) {
        super.execute(new Props(parentProperties, props), callback);
    }
}
