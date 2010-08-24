package azkaban.flow;

import azkaban.common.utils.Props;
import org.joda.time.DateTime;

import java.util.List;

/**
 */
public class PropertyPushingExecutableFlow implements ExecutableFlow
{
    private final String id;
    private final String name;
    private final Props props;
    private final GroupedExecutableFlow subFlow;

    public PropertyPushingExecutableFlow(
            String id,
            String name,
            Props props,
            ExecutableFlow... children
    )
    {
        this.id = id;
        this.name = name;
        this.props = props;
        this.subFlow = new GroupedExecutableFlow(name + "-subflow", children);
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
        subFlow.execute(new Props(parentProperties, props), callback);
    }

    @Override
    public boolean cancel() {
        return subFlow.cancel();
    }

    @Override
    public Status getStatus() {
        return subFlow.getStatus();
    }

    @Override
    public boolean reset() {
        return subFlow.reset();
    }

    @Override
    public boolean markCompleted() {
        return subFlow.markCompleted();
    }

    @Override
    public boolean hasChildren() {
        return subFlow.hasChildren();
    }

    @Override
    public List<ExecutableFlow> getChildren() {
        return subFlow.getChildren();
    }

    @Override
    public DateTime getStartTime() {
        return subFlow.getStartTime();
    }

    @Override
    public DateTime getEndTime() {
        return subFlow.getEndTime();
    }

    @Override
    public Props getParentProps() {
        return subFlow.getParentProps();
    }

    @Override
    public Throwable getException() {
        return subFlow.getException();
    }
}
