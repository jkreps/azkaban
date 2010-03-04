package azkaban.flow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class MultipleDependencyFlow implements Flow
{
    private final GroupedFlow dependeesGrouping;
    private final ComposedFlow actualFlow;
    private final Flow depender;

    public MultipleDependencyFlow(Flow depender, Flow... dependees)
    {
        this.depender = depender;
        dependeesGrouping = new GroupedFlow(dependees);
        actualFlow = new ComposedFlow(this.depender, dependeesGrouping);
    }

    @Override
    public String getName()
    {
        return actualFlow.getName();
    }

    @Override
    public boolean hasChildren()
    {
        return true;
    }

    @Override
    public List<Flow> getChildren()
    {
        return dependeesGrouping.getChildren();
    }

    @Override
    public ExecutableFlow createExecutableFlow(String id, Map<String, ExecutableFlow> overrides)
    {
        final List<ExecutableFlow> childList = dependeesGrouping.createExecutableFlow(id, overrides).getChildren();
        ExecutableFlow[] executableChildren = childList.toArray(new ExecutableFlow[childList.size()]);

        final ExecutableFlow dependerFlow = overrides.containsKey(depender.getName()) ?
                                            overrides.get(depender.getName()) :
                                            depender.createExecutableFlow(id, new HashMap<String, ExecutableFlow>());


        final MultipleDependencyExecutableFlow retVal = new MultipleDependencyExecutableFlow(id, dependerFlow, executableChildren);

        if (overrides.containsKey(retVal.getName())) {
            throw new RuntimeException(String.format("overrides already has an entry with my key[%s], wtf?", retVal.getName()));
        }
        overrides.put(retVal.getName(), retVal);

        return retVal;
    }

    @Override
    public String toString()
    {
        return "MultipleDependencyExecutableFlow{" +
               "dependeesGrouping=" + dependeesGrouping +
               ", actualFlow=" + actualFlow +
               '}';
    }
}