package azkaban.flow;

import azkaban.common.utils.Props;

/**
 */
public class FlowExecutionHolder {
    private final ExecutableFlow flow;
    private final Props parentProps;

    public FlowExecutionHolder(ExecutableFlow flow, Props parentProps) {
        this.flow = flow;
        this.parentProps = parentProps;
    }

    public ExecutableFlow getFlow() {
        return flow;
    }

    public Props getParentProps() {
        return parentProps;
    }
}
