package azkaban.flow.manager;

import azkaban.flow.ExecutableFlow;
import azkaban.flow.Flow;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 *
 */
public interface FlowManager extends Iterable<Flow>
{
    boolean hasFlow(String name);

    Flow getFlow(String name);

    Collection<Flow> getFlows();

    Set<String> getRootFlowNames();

    @Override
    Iterator<Flow> iterator();

    ExecutableFlow createNewExecutableFlow(String name);

    long getNextId();

    long getCurrMaxId();

    ExecutableFlow saveExecutableFlow(ExecutableFlow flow);

    ExecutableFlow loadExecutableFlow(long id);

    void reload();
}
