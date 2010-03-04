package azkaban.serialization.de;

import azkaban.flow.ExecutableFlow;
import azkaban.serialization.Verifier;
import com.google.common.base.Function;

import java.util.Map;

/**
 *
 */
public class JobFlowDeserializer implements Function<Map<String, Object>, ExecutableFlow>
{
    private final Map<String, Function<Map<String, Object>, ExecutableFlow>> baseDeserializerMap;

    public JobFlowDeserializer(Map<String, Function<Map<String, Object>, ExecutableFlow>> baseDeserializerMap)
    {

        this.baseDeserializerMap = baseDeserializerMap;
    }

    @Override
    public ExecutableFlow apply(Map<String, Object> descriptor)
    {
        String type = Verifier.getString(descriptor, "type");

        Function<Map<String, Object>, ExecutableFlow> deserializer = baseDeserializerMap.get(type);
        if (deserializer == null) {
            throw new RuntimeException(
                    String.format(
                            "No deserializer for type[%s].  Known types are [%s]",
                            type,
                            baseDeserializerMap.keySet())
            );                    
        }

        return deserializer.apply(descriptor);
    }
}
