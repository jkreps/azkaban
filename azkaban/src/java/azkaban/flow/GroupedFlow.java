package azkaban.flow;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class GroupedFlow implements Flow
{
    private final Flow[] flows;
    private final Flow[] sortedFlows;

    public GroupedFlow(Flow... flows)
    {
        this.flows = flows;
        this.sortedFlows = Arrays.copyOf(this.flows, this.flows.length);
        Arrays.sort(this.sortedFlows, new Comparator<Flow>()
        {
            @Override
            public int compare(Flow o1, Flow o2)
            {
                return o1.getName().compareTo(o2.getName());
            }
        });
    }

    @Override
    public String getName()
    {
        return StringUtils.join(
                Iterables.transform(Arrays.asList(flows), new Function<Flow, String>()
                {
                    @Override
                    public String apply(Flow flow)
                    {
                        return flow.getName();
                    }
                }).iterator(),
                " + "
        );
    }

    @Override
    public boolean hasChildren()
    {
        return true;
    }

    @Override
    public List<Flow> getChildren()
    {
        return Arrays.asList(sortedFlows);
    }

    @Override
    public ExecutableFlow createExecutableFlow(String id, Map<String, ExecutableFlow> overrides)
    {
        ExecutableFlow executableFlows[] = new ExecutableFlow[flows.length];

        for (int i = 0; i < executableFlows.length; i++) {
            executableFlows[i] = overrides.containsKey(flows[i].getName()) ?
                                 overrides.get(flows[i].getName()) :
                                 flows[i].createExecutableFlow(id, overrides);
        }

        // Grouped(Executable)Flow is just an abstraction, it doesn't represent anything concrete and thus shouldn't
        // be put in the overrides map.

        return new GroupedExecutableFlow(id, executableFlows);
    }

    @Override
    public String toString()
    {
        return "GroupedExecutableFlow{" +
               "flows=" + (flows == null ? null : Arrays.asList(flows)) +
               '}';
    }
}