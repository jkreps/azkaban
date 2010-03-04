package azkaban.serialization;

import azkaban.flow.*;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 *
 */
public class DefaultExecutableFlowSerializer extends ExecutableFlowSerializer
{
    public DefaultExecutableFlowSerializer()
    {
        Map<Class, Function<ExecutableFlow, Map<String, Object>>> subSerializers =
                ImmutableMap.<Class, Function<ExecutableFlow, Map<String, Object>>>of(
                        IndividualJobExecutableFlow.class, new IndividualJobEFSerializer(),
                        GroupedExecutableFlow.class, new GroupedEFSerializer(this),
                        ComposedExecutableFlow.class, new ComposedEFSerializer(this),
                        MultipleDependencyExecutableFlow.class, new MultipleDependencyEFSerializer(this)
                );

        setSerializers(subSerializers);
    }
}
