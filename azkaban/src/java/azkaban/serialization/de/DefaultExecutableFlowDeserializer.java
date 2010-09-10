package azkaban.serialization.de;

import azkaban.app.JobManager;
import azkaban.app.JobWrappingFactory;
import azkaban.flow.ExecutableFlow;
import azkaban.flow.JobManagerFlowDeserializer;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 */
public class DefaultExecutableFlowDeserializer extends ExecutableFlowDeserializer
{
    public DefaultExecutableFlowDeserializer(
            final JobManager jobManager,
            final JobWrappingFactory jobFactory
    )
    {
        setJobDeserializer(
                new JobFlowDeserializer(
                        ImmutableMap.<String, Function<Map<String, Object>, ExecutableFlow>>of(
                                "jobManagerLoaded", new JobManagerFlowDeserializer(jobManager, jobFactory)
                        )
                )
        );
    }
}
