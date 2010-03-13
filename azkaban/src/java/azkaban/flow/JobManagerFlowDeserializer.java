package azkaban.flow;

import azkaban.app.JobFactory;
import azkaban.app.JobManager;
import azkaban.flow.ExecutableFlow;
import azkaban.flow.IndividualJobExecutableFlow;
import azkaban.serialization.Verifier;
import com.google.common.base.Function;
import org.joda.time.DateTime;

import java.util.Map;

/**
 *
 */
public class JobManagerFlowDeserializer implements Function<Map<String, Object>, ExecutableFlow>
{
    private final JobManager jobManager;
    private final JobFactory jobFactory;

    public JobManagerFlowDeserializer(
            JobManager jobManager,
            JobFactory jobFactory
    )
    {
        this.jobManager = jobManager;
        this.jobFactory = jobFactory;
    }

    @Override
    public ExecutableFlow apply(Map<String, Object> descriptor)
    {
        String jobName = Verifier.getString(descriptor, "name");
        Status jobStatus = Verifier.getEnumType(descriptor, "status", Status.class);
        String id = Verifier.getString(descriptor, "id");
        DateTime startTime = Verifier.getOptionalDateTime(descriptor, "startTime");
        DateTime endTime = Verifier.getOptionalDateTime(descriptor, "endTime");

        final IndividualJobExecutableFlow retVal = new IndividualJobExecutableFlow(id, jobFactory, jobManager.getJobDescriptor(jobName));
        retVal.setStatus(jobStatus);

        if (startTime != null) {
            retVal.setStartTime(startTime);
        }

        if (endTime != null) {
            retVal.setEndTime(endTime);
        }

        return retVal;
    }
}
