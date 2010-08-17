package azkaban.flow;

import azkaban.common.utils.Props;
import org.joda.time.DateTime;

import java.util.List;

/**
 */
public class WrappingExecutableFlow implements ExecutableFlow
{
    private final ExecutableFlow delegateFlow;

    public WrappingExecutableFlow(ExecutableFlow delegateFlow) {
        this.delegateFlow = delegateFlow;
    }

    public String getId() {
        return delegateFlow.getId();
    }

    public String getName() {
        return delegateFlow.getName();
    }

    public void execute(Props parentProperties, FlowCallback callback) {
        delegateFlow.execute(parentProperties, callback);
    }

    public boolean cancel() {
        return delegateFlow.cancel();
    }

    public Status getStatus() {
        return delegateFlow.getStatus();
    }

    public boolean reset() {
        return delegateFlow.reset();
    }

    public boolean markCompleted() {
        return delegateFlow.markCompleted();
    }

    public boolean hasChildren() {
        return delegateFlow.hasChildren();
    }

    public List<ExecutableFlow> getChildren() {
        return delegateFlow.getChildren();
    }

    public DateTime getStartTime() {
        return delegateFlow.getStartTime();
    }

    public DateTime getEndTime() {
        return delegateFlow.getEndTime();
    }

    public Props getParentProps() {
        return delegateFlow.getParentProps();
    }

    public Throwable getException() {
        return delegateFlow.getException();
    }
}
