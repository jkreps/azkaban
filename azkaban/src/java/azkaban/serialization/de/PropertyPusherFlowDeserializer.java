package azkaban.serialization.de;

import azkaban.common.utils.Props;
import azkaban.flow.ExecutableFlow;
import azkaban.flow.PropertyPusherExecutableFlow;
import azkaban.serialization.Verifier;
import com.google.common.base.Function;

import java.util.List;
import java.util.Map;

/**
 */
public class PropertyPusherFlowDeserializer implements Function<Map<String, Object>, ExecutableFlow>
{
    private final ExecutableFlowDeserializer globalDeserializer;

    public PropertyPusherFlowDeserializer(
            ExecutableFlowDeserializer globalDeserializer
    )
    {
        this.globalDeserializer = globalDeserializer;
    }

    @Override
    public ExecutableFlow apply(Map<String, Object> jobDescriptor) {
        final ExecutableFlow propGenFlow = globalDeserializer.apply(Verifier.getVerifiedObject(jobDescriptor, "propGenFlow", Map.class));
        final ExecutableFlow subFlow = globalDeserializer.apply(Verifier.getVerifiedObject(jobDescriptor, "subFlow", Map.class));
        final List<ExecutableFlow> subFlowChildren = subFlow.getChildren();

        return new PropertyPusherExecutableFlow(
                Verifier.getString(jobDescriptor, "id"),
                Verifier.getString(jobDescriptor, "name"),
                propGenFlow,
                subFlowChildren.toArray(new ExecutableFlow[subFlowChildren.size()])
        );
    }
}
