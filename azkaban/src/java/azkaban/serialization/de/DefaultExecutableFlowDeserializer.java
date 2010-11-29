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
