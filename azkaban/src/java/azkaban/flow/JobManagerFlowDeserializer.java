/*
 * Copyright 2010 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

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
