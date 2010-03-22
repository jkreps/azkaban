package azkaban.flow;

import azkaban.app.JobFactory;
import azkaban.app.JobManager;
import azkaban.app.JobWrappingFactory;
import azkaban.app.LazyJobFactory;
import azkaban.common.jobs.Job;
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
    private final JobWrappingFactory jobFactory;

    public JobManagerFlowDeserializer(
            JobManager jobManager,
            JobWrappingFactory jobFactory
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

        final IndividualJobExecutableFlow retVal = new IndividualJobExecutableFlow(
                id,
                jobName,
                new LazyJobFactory(jobFactory, jobManager, jobName)
        );
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
