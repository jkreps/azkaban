package azkaban.serialization;

import azkaban.flow.ExecutableFlow;
import azkaban.flow.MultipleDependencyExecutableFlow;
import com.google.common.base.Function;

import java.util.Map;

/**
 *
 */
public class MultipleDependencyEFSerializer implements Function<ExecutableFlow, Map<String, Object>>
{
    private final Function<ExecutableFlow, Map<String, Object>> globalSerializer;

    public MultipleDependencyEFSerializer(Function<ExecutableFlow, Map<String, Object>> globalSerializer)
    {
        this.globalSerializer = globalSerializer;
    }

    @Override
    public Map<String, Object> apply(ExecutableFlow executableFlow)
    {
        MultipleDependencyExecutableFlow flow = (MultipleDependencyExecutableFlow) executableFlow;

        return globalSerializer.apply(flow.getActualFlow());
    }
}
