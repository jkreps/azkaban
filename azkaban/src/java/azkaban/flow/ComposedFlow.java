package azkaban.flow;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class ComposedFlow implements Flow
{
    private final Flow depender;
    private final Flow dependee;

    public ComposedFlow(Flow depender, Flow dependee)
    {
        this.depender = depender;
        this.dependee = dependee;
    }

    @Override
    public String getName()
    {
        return depender.getName();
    }

    @Override
    public boolean hasChildren()
    {
        return true;
    }

    @Override
    public List<Flow> getChildren()
    {
        return Arrays.asList(dependee);
    }

    @Override
    public ExecutableFlow createExecutableFlow(String id, Map<String, ExecutableFlow> overrides)
    {
        final ExecutableFlow dependeeFlow = overrides.containsKey(dependee.getName()) ?
                                            overrides.get(dependee.getName()) :
                                            dependee.createExecutableFlow(id, overrides);

        final ExecutableFlow dependerFlow = overrides.containsKey(depender.getName()) ?
                                            overrides.get(depender.getName()) :
                                            depender.createExecutableFlow(id, new HashMap<String, ExecutableFlow>());

        final ComposedExecutableFlow retVal = new ComposedExecutableFlow(id, dependerFlow, dependeeFlow);

        if (overrides.containsKey(retVal.getName())) {
            throw new RuntimeException(String.format("overrides already has an entry with my key[%s], wtf?", retVal.getName()));
        }
        overrides.put(retVal.getName(), retVal);

        return retVal;
    }

    @Override
    public String toString()
    {
        return "ComposedExecutableFlow{" +
               "depender=" + depender +
               ", dependee=" + dependee +
               '}';
    }
}