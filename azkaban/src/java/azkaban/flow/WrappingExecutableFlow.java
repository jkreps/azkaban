package azkaban.flow;

import azkaban.common.utils.Props;
import azkaban.jobs.Status;

import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;

/**
 * A convenience class to make it easier to inject behavior
 * into a single method call for an ExecutableFlow.
 */
public class WrappingExecutableFlow implements ExecutableFlow
{
    private final ExecutableFlow delegateFlow;

    public WrappingExecutableFlow(ExecutableFlow delegateFlow) {
        this.delegateFlow = delegateFlow;
    }

    @Override
    public String getId() {
        return delegateFlow.getId();
    }

    @Override
    public String getName() {
        return delegateFlow.getName();
    }

    @Override
    public void execute(Props parentProperties, FlowCallback callback) {
        delegateFlow.execute(parentProperties, callback);
    }

    @Override
    public boolean cancel() {
        return delegateFlow.cancel();
    }

    @Override
    public Status getStatus() {
        return delegateFlow.getStatus();
    }

    @Override
    public boolean reset() {
        return delegateFlow.reset();
    }

    @Override
    public boolean markCompleted() {
        return delegateFlow.markCompleted();
    }

    @Override
    public boolean hasChildren() {
        return delegateFlow.hasChildren();
    }

    @Override
    public List<ExecutableFlow> getChildren() {
        return delegateFlow.getChildren();
    }

    @Override
    public DateTime getStartTime() {
        return delegateFlow.getStartTime();
    }

    @Override
    public DateTime getEndTime() {
        return delegateFlow.getEndTime();
    }

    @Override
    public Props getParentProps() {
        return delegateFlow.getParentProps();
    }

    @Override
    public Props getReturnProps() {
        return delegateFlow.getReturnProps();
    }

    @Override
    public Map<String, Throwable> getExceptions() {
        return delegateFlow.getExceptions();
    }

    public ExecutableFlow getDelegateFlow()
    {
        return delegateFlow;
    }
}
