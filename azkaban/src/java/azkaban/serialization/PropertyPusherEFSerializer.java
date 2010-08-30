package azkaban.serialization;

import azkaban.flow.ExecutableFlow;
import azkaban.flow.PropertyPusherExecutableFlow;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class PropertyPusherEFSerializer implements Function<ExecutableFlow, Map<String, Object>>
{
    private final Function<ExecutableFlow, Map<String, Object>> globalSerializer;

    public PropertyPusherEFSerializer(Function<ExecutableFlow, Map<String, Object>> globalSerializer)
    {
        this.globalSerializer = globalSerializer;
    }

    @Override
    public Map<String, Object> apply(ExecutableFlow executableFlow)
    {
        PropertyPusherExecutableFlow flow = (PropertyPusherExecutableFlow) executableFlow;

        Map<String, Object> retVal = new HashMap<String, Object>();

        Map<String, Object> thisDescriptor = new HashMap<String, Object>();
        thisDescriptor.put("type", "propertyPusher");
        thisDescriptor.put("id", flow.getId());
        thisDescriptor.put("name", flow.getName());
        thisDescriptor.put("propGenFlow", globalSerializer.apply(flow.getPropertyGeneratingFlow()));
        thisDescriptor.put("subFlow", globalSerializer.apply(flow.getSubFlow()));


        retVal.put("jobs", ImmutableMap.of(flow.getName(), thisDescriptor));
        retVal.put("root", Arrays.asList(flow.getName()));
        retVal.put("dependencies", Collections.<String, Object>emptyMap());
        retVal.put("id", flow.getId());
        return retVal;
    }
}
