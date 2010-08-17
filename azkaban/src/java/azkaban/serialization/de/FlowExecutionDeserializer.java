package azkaban.serialization.de;

import azkaban.common.utils.Props;
import azkaban.flow.FlowExecutionHolder;
import azkaban.serialization.Verifier;
import com.google.common.base.Function;

import java.util.Map;

/**
 */
public class FlowExecutionDeserializer implements Function<Map<String, Object>, FlowExecutionHolder>
{
    private final ExecutableFlowDeserializer flowDeserializer;

    public FlowExecutionDeserializer(
            ExecutableFlowDeserializer flowDeserializer
    )
    {
        this.flowDeserializer = flowDeserializer;
    }

    @Override
    public FlowExecutionHolder apply(Map<String, Object> descriptor) {
        final FlowExecutionHolder retVal;

        // Maintain backwards compatibility, check if it's really a holder
        final Object type = descriptor.get("type");
        if (type != null && "FlowExecutionHolder".equals(type.toString())) {
            retVal = new FlowExecutionHolder(
                    flowDeserializer.apply(Verifier.getVerifiedObject(descriptor, "flow", Map.class)),
                    new Props(new Props(), Verifier.getVerifiedObject(descriptor, "parentProps", Map.class))
            );
        }
        else {
            retVal = new FlowExecutionHolder(
                    flowDeserializer.apply(descriptor),
                    new Props()
            );
        }

        return retVal;
    }
}
